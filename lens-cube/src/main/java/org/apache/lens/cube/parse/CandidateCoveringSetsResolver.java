package org.apache.lens.cube.parse;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.lens.cube.error.LensCubeErrorCode;
import org.apache.lens.cube.metadata.TimeRange;

import org.apache.lens.server.api.error.LensException;

import java.util.*;

@Slf4j
public class CandidateCoveringSetsResolver implements ContextRewriter {

  private List<UnionCandidate> unionCandidates = new ArrayList<>();
  private List<Candidate> finalCandidates = new ArrayList<>();

  public CandidateCoveringSetsResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws LensException {

    Set<QueriedPhraseContext> queriedMsrs = new HashSet<>();
    for (QueriedPhraseContext qur : cubeql.getQueriedPhrases()) {
      if (qur.hasMeasures(cubeql)) {
        queriedMsrs.add(qur);
      }
    }
    // if no measures are queried, add all StorageCandidates individually as single covering sets
    if (queriedMsrs.isEmpty()) {
      finalCandidates.addAll(cubeql.getCandidateSet());
    }

    List<TimeRange> ranges = cubeql.getTimeRanges();
    // considering single time range
    TimeRange range = ranges.iterator().next();
    resolveRangeCoveringFactSet(cubeql, range, queriedMsrs);
    List<List<UnionCandidate>> measureCoveringSets = resolveJoinCandidates(unionCandidates, queriedMsrs, cubeql);
    updateFinalCandidates(measureCoveringSets);
    log.info("Covering candidate sets :{}", finalCandidates);

    String msrString = CandidateUtil.getColumns(queriedMsrs).toString();
    if (finalCandidates.isEmpty()) {
      throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(), msrString);
    }
    // update final candidate sets
    cubeql.getCandidateSet().clear();
    cubeql.getCandidateSet().addAll(finalCandidates);
    // TODO : we might need to prune if we maintian two data structures in CubeQueryContext.
    //cubeql.pruneCandidateFactWithCandidateSet(CandidateTablePruneCause.columnNotFound(getColumns(queriedMsrs)));

    if (cubeql.getCandidateFacts().size() == 0) {
      throw new LensException(LensCubeErrorCode.NO_FACT_HAS_COLUMN.getLensErrorInfo(), msrString);
    }
  }

  private Candidate createJoinCandidateFromUnionCandidates(List<UnionCandidate> ucs) {
    Candidate cand;
    if (ucs.size() >= 2) {
      UnionCandidate first = ucs.get(0);
      UnionCandidate second = ucs.get(1);
      cand = new JoinCandidate(first, second);
      for (int i = 2; i < ucs.size(); i++) {
        cand = new JoinCandidate(cand, ucs.get(i));
      }
    } else {
      cand = ucs.get(0);
    }
    return cand;
  }

  private void updateFinalCandidates(List<List<UnionCandidate>> jcs) {

    for (Iterator<List<UnionCandidate>> itr = jcs.iterator(); itr.hasNext(); ) {
      List<UnionCandidate> jc = itr.next();
      if (jc.size() == 1 && jc.iterator().next().getChildCandidates().size() == 1) {
        finalCandidates.add(jc.iterator().next().getChildCandidates().iterator().next());
      } else {
        finalCandidates.add(createJoinCandidateFromUnionCandidates(jc));
      }
    }
  }

  private void resolveRangeCoveringFactSet(CubeQueryContext cubeql, TimeRange range,
                                           Set<QueriedPhraseContext> queriedMsrs) throws LensException {
    // All facts
    List<Candidate> allCandidates = cubeql.getCandidateSet();
    // Partially valid facts
    List<Candidate> allCandidatesPartiallyValid = new ArrayList<>();
    for (Candidate cand : allCandidates) {
      // Assuming initial list of candidates populated are StorageCandidate
      if (cand instanceof StorageCandidate) {
        StorageCandidate sc = (StorageCandidate) cand;
        if (CandidateUtil.isValidForTimeRange(sc, range)) {
          List<Candidate> one = new ArrayList<Candidate>(Arrays.asList(sc));
          unionCandidates.add(new UnionCandidate(one));
        } else if (CandidateUtil.isPartiallyValidForTimeRange(sc, range)) {
          allCandidatesPartiallyValid.add(sc);
        }
      } else {
        throw new LensException("Not a StorageCandidate!!");
      }
    }
    // Get all covering fact sets
    List<List<Candidate>> coveringFactSets =
        getCombinations(new ArrayList<Candidate>(allCandidatesPartiallyValid));
    // Sort the Collection based on size
    Collections.sort(coveringFactSets, new Comparator<List<?>>() {
      @Override
      public int compare(List<?> o1, List<?> o2) {
        return Integer.valueOf(o1.size()).compareTo(o2.size());
      }
    });

    // iterate over the candidate set and remove the one which can't answer the range
    for (Iterator<List<Candidate>> itr = coveringFactSets.iterator(); itr.hasNext(); ) {
      List<Candidate> cand = itr.next();
      if (!CandidateUtil.isTimeRangeCovered(cand, range.getFromDate(), range.getToDate())) {
        itr.remove();
      }
    }
    // prune candidate set without common measure
    pruneCoveringSetWithoutAnyCommonMeasure(coveringFactSets, queriedMsrs, cubeql);
    // prune redundant covering sets
    pruneRedundantCoveringSets(coveringFactSets);
    // pruing done in the previous steps, now create union candidates
    for (List<Candidate> set : coveringFactSets) {
      UnionCandidate uc = new UnionCandidate(set);
      unionCandidates.add(uc);
    }
  }

  private boolean isMeasureAnswerableForCandidates(QueriedPhraseContext msr, List<Candidate> candList,
                                                   CubeQueryContext cubeql) throws LensException {
    for (Candidate cand : candList) {
      if (!msr.isEvaluable(cubeql, (StorageCandidate) cand)) {
        return false;
      }
    }
    return true;
  }

  private void pruneCoveringSetWithoutAnyCommonMeasure(List<List<Candidate>> candidates,
                                                       Set<QueriedPhraseContext> queriedMsrs,
                                                       CubeQueryContext cubeql) throws LensException {
    for (ListIterator<List<Candidate>> itr = candidates.listIterator(); itr.hasNext(); ) {
      boolean toRemove = true;
      List<Candidate> cand = itr.next();
      for (QueriedPhraseContext msr : queriedMsrs) {
        if (isMeasureAnswerableForCandidates(msr, cand, cubeql)) {
          toRemove = false;
          break;
        }
      }
      if (toRemove) {
        itr.remove();
      }
    }
  }

  private void pruneRedundantCoveringSets(List<List<Candidate>> candidates) {
    for (int i = 0; i < candidates.size(); i++) {
      List<Candidate> current = candidates.get(i);
      int j = i + 1;
      for (ListIterator<List<Candidate>> itr = candidates.listIterator(j); itr.hasNext(); ) {
        List<Candidate> next = itr.next();
        if (next.containsAll(current)) {
          itr.remove();
        }
      }
    }
  }

  public List<List<Candidate>> getCombinations(final List<Candidate> candidates) {
    List<List<Candidate>> combinations = new LinkedList<List<Candidate>>();
    int size = candidates.size();
    int threshold = Double.valueOf(Math.pow(2, size)).intValue() - 1;

    for (int i = 1; i <= threshold; ++i) {
      LinkedList<Candidate> individualCombinationList = new LinkedList<Candidate>();
      int count = size - 1;
      int clonedI = i;
      while (count >= 0) {
        if ((clonedI & 1) != 0) {
          individualCombinationList.addFirst(candidates.get(count));
        }
        clonedI = clonedI >>> 1;
        --count;
      }
      combinations.add(individualCombinationList);
    }
    return combinations;
  }

  private List<List<UnionCandidate>> resolveJoinCandidates(List<UnionCandidate> unionCandidates,
                                                           Set<QueriedPhraseContext> msrs,
                                                           CubeQueryContext cubeql) throws LensException {
    List<List<UnionCandidate>> msrCoveringSets = new ArrayList<>();
    List<UnionCandidate> ucSet = new ArrayList<>(unionCandidates);
    boolean evaluable = false;
    // Check if a single set can answer all the measures and exprsWithMeasures
    for (Iterator<UnionCandidate> i = ucSet.iterator(); i.hasNext(); ) {
      UnionCandidate uc = i.next();
      for (QueriedPhraseContext msr : msrs) {
        evaluable = isMeasureAnswerableForCandidates(msr, uc.getChildCandidates(), cubeql) ? true : false;
        if (!evaluable) {
          break;
        }
      }
      if (evaluable) {
        // single set can answer all the measures as an UnionCandidate
        List<UnionCandidate> one = new ArrayList<>();
        one.add(uc);
        msrCoveringSets.add(one);
        i.remove();
      }
    }
    // Sets that contain all measures or no measures are removed from iteration.
    // find other facts
    for (Iterator<UnionCandidate> i = ucSet.iterator(); i.hasNext(); ) {
      UnionCandidate uc = i.next();
      i.remove();
      // find the remaining measures in other facts
      if (i.hasNext()) {
        Set<QueriedPhraseContext> remainingMsrs = new HashSet<>(msrs);
        Set<QueriedPhraseContext> coveredMsrs = CandidateUtil.coveredMeasures(uc.getChildCandidates(), msrs, cubeql);
        remainingMsrs.removeAll(coveredMsrs);

        List<List<UnionCandidate>> coveringSets = resolveJoinCandidates(ucSet, remainingMsrs, cubeql);
        if (!coveringSets.isEmpty()) {
          for (List<UnionCandidate> candSet : coveringSets) {
            candSet.add(uc);
            msrCoveringSets.add(candSet);
          }
        } else {
          log.info("Couldnt find any set containing remaining measures:{} {} in {}", remainingMsrs,
              ucSet);
        }
      }
    }
    log.info("Covering set {} for measures {} with factsPassed {}", msrCoveringSets, msrs, ucSet);
    return msrCoveringSets;
  }
}