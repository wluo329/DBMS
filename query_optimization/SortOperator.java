package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double)numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement
        //return empty run
        if (!records.hasNext()) {
            return makeRun();
        }

        ArrayList sorted_list = new ArrayList<>();
        while (records.hasNext()) {
            sorted_list.add(records.next());
            Collections.sort(sorted_list, this.comparator);
        }
        return makeRun(sorted_list);
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        // TODO(proj3_part1): implement
        RecordPairComparator rpc = new RecordPairComparator();
        PriorityQueue<Pair<Record, Integer>> pQueue = new PriorityQueue<>(rpc);

        //list of the iterators for each run in runs
        ArrayList<BacktrackingIterator<Record>> iter_list = new ArrayList<>();

        //store all records in runs iterator form
        for (Run r : runs) { iter_list.add(r.iterator()); }

        //put all record in pqueue
        for (int i = 0; i < runs.size(); i++)  {
            if(iter_list.get(i).hasNext()) {
                Pair<Record, Integer> rec = new Pair<>(iter_list.get(i).next(), i);
                pQueue.add(rec);
            }
        }

        Run new_run = makeRun();

        //contains record and run i
        Pair<Record, Integer> rec_pair;

        //merge runs
        while (!pQueue.isEmpty()) {
            //get first record from pq
            rec_pair = pQueue.poll();
            //add record to new run
            new_run.add(rec_pair.getFirst());
            int run_index = rec_pair.getSecond();
            //did we reach the end of the iterator
            if (iter_list.get(run_index).hasNext()) {
                //add pair with next record and index of run if we didn't reach end of current iterator
                Pair<Record, Integer> new_rec_pair = new Pair<>(iter_list.get(run_index).next(), run_index);
                pQueue.add(new_rec_pair);
            }
        }

        return new_run;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        List<Run> sortedRunsList = new ArrayList<>();

        int totalSortedRuns = runs.size();
        int availableBuffers = numBuffers - 1;

        //combine availableBuffers runs into 1 run
        for (int i = 0; i + availableBuffers <= totalSortedRuns; i += availableBuffers) {
            //get a sorted run from combining previous sorted runs
            //last sorted run will be result of merging less than available buffer runs
            Run sortedRun = mergeSortedRuns(runs.subList(i, Math.min(availableBuffers + i, runs.size())));
            sortedRunsList.add(sortedRun);
        }

        return sortedRunsList;
        //return Collections.emptyList();
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();

//        * @return This method will consume up to `maxPages` pages of records from
//     * `records` (advancing it in the process) and return a backtracking
//                * iterator over those records. Setting maxPages to 1 will result in an
//     * iterator over a single page of records.
        // TODO(proj3_part1): implement

        List <Run>mergedRunList = new ArrayList<>();

        //Pass 0: use b buffer pages to create N/B sorted runs of B pages each
        while (sourceIterator.hasNext()) {
            //create a sorted run from sourceIterator with max buffers
            Run mergedRun = sortRun(getBlockIterator(sourceIterator, this.getSchema(), numBuffers));
            mergedRunList.add(mergedRun);
        }

        //Pass 1 and forward: merge B - 1 sorted runs
        //for each sorted run in the current pass, merge
        //stop when we have 1 sorted run
        while (mergedRunList.size() != 1) {
            mergedRunList = mergePass(mergedRunList);
        }

        //at this point, we have one sorted run with N pages.
        return mergedRunList.get(0);

        //return makeRun(); // TODO(proj3_part1): replace this!
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

