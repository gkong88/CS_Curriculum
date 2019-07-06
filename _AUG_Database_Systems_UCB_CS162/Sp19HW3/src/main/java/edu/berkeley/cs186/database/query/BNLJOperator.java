package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;

public class BNLJOperator extends JoinOperator {
    private int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getNumMemoryPages();

        // for HW4
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new BNLJIterator();
    }

    public int estimateIOCost() {
        //This method implements the the IO cost estimation of the Block Nested Loop Join

        int usableBuffers = numBuffers -
                            2; //Common mistake have to first calculate the number of usable buffers

        int numLeftPages = getLeftSource().getStats().getNumPages();

        int numRightPages = getRightSource().getStats().getNumPages();

        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;

    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might prove to be a useful reference).
     */

    private class BNLJIterator extends JoinIterator {
        /**
         * Some member variables are provided for guidance, but there are many possible solutions.
         * You should implement the solution that's best for you, using any member variables you need.
         * You're free to use these member variables, but you're not obligated to.
         */

        private Iterator<Page> leftIterator = null;
        private Iterator<Page> rightIterator = null;
        private BacktrackingIterator<Record> leftRecordIterator = null;
        private BacktrackingIterator<Record> rightRecordIterator = null;
        private Record leftRecord = null;
        private Record rightRecord = null;
        private Record nextRecord = null;
        int usable_buffers = numBuffers - 2;

        public BNLJIterator() throws QueryPlanException, DatabaseException {
            super();
            leftIterator = BNLJOperator.this.getPageIterator(getLeftTableName());
            rightIterator = BNLJOperator.this.getPageIterator(getRightTableName());
            leftIterator.next(); //throw away header page
            rightIterator.next(); //throw away header page
            leftRecordIterator = getBlockIterator(getLeftTableName(), leftIterator, usable_buffers);
            rightRecordIterator = getBlockIterator(getRightTableName(), rightIterator, 1);
            leftRecordIterator.mark();
            rightRecordIterator.mark();
            leftRecord = leftRecordIterator.hasNext() ? leftRecordIterator.next() : null;
            rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
            try {
                fetchNextRecord();
            } catch (DatabaseException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            return nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Record result = nextRecord;
            try {
                fetchNextRecord();
            } catch (DatabaseException e) {
                nextRecord = null;
            }
            return result;
        }

        /**
         * pre-fetches next record this iterator produces.
         * If none exists, stores a null value.
         */
        private void fetchNextRecord() throws DatabaseException {
            if (this.leftRecord == null) { throw new DatabaseException("No new record to fetch"); }
            nextRecord = null;
            // keep searching until we've exhausted the search space or we find a match.
            // left is only null IFF all record pairs have been checked
            while (leftRecord != null &&  nextRecord == null) {
                if (leftRecord != null && rightRecord != null) {
                    DataBox leftJoinValue = leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
                        List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                    }
                    rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                }
                else if (leftRecord != null && rightRecord == null) {
                    finishedRightBlock();
                }
            }
        }

        private void finishedRightBlock() throws DatabaseException {
            // invariant: current left record has been checked against all right records of the active page
            if (leftRecordIterator.hasNext()) {
                // there's still more left records in the left page.
                // reset the right page and check all right records in this page against the left record
                // while it is still in memory.
                rightRecordIterator.reset();
                leftRecord = leftRecordIterator.next();
                rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
            }
            else {
                // invariant: all left records has been checked against all right records in the current respecitve pages
                if (rightIterator.hasNext()) {
                    // invariant: there's still more right pages that the current left page hasn't been checked against
                    // Reset the left page
                    leftRecordIterator.reset();
                    //  Get the next right page
                    rightRecordIterator = getBlockIterator(getRightTableName(), rightIterator, 1);
                    rightRecordIterator.mark();

                    leftRecord = leftRecordIterator.next();
                    rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                } else {
                    //invariant: the left page has been checked against all right records in all the right pages
                    if (leftIterator.hasNext()) {
                        // move to the next left page
                        leftRecordIterator = getBlockIterator(getLeftTableName(), new Page[]{leftIterator.next()});
                        leftRecordIterator.mark();

                        // reset the the rightIterator and get the first right page
                        rightIterator = BNLJOperator.this.getPageIterator(getRightTableName());
                        rightIterator.next(); //throw away header page
                        rightRecordIterator = getBlockIterator(getRightTableName(), rightIterator, usable_buffers);

                        rightRecordIterator.mark();

                        rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                        leftRecord = leftRecordIterator.next();
                    } else {
                        leftRecord = null;
                    }
                }
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}
