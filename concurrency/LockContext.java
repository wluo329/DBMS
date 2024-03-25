package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (parent != null) {
            if (!validRequest(transaction, lockType)) {
                throw new InvalidLockException("Invalid Request");
            }
        }
        if (hasSIXAncestor(transaction) && (lockType == LockType.S || lockType == LockType.IS)) {
            throw new InvalidLockException("Invalid Request");
        }

        if(getExplicitLockType(transaction) != lockType.NL) {
            throw new DuplicateLockRequestException("Duplicate Request.");
        }
        if(readonly) {
            throw new UnsupportedOperationException("Context is readonly.");
        }

        //acquire lock for transaction
        lockman.acquire(transaction, name, lockType);

        //update numChildLocks
        incChildLocks(transaction, parent, 1);
        return;
    }

    private boolean validRequest(TransactionContext transaction, LockType lock) {
        LockType pLock = LockType.parentLock(lock);
        List<Lock> locks = lockman.getLocks(parent.name);
        List<Lock> XactLocks = new ArrayList<>();

        //get all parent locks of the relevant transaction
        for (Lock l : locks) {
            if (l.transactionNum == transaction.getTransNum()) {
                XactLocks.add(l);
            }
        }

        //look into all of the transaction parent's locks
        for (Lock l : XactLocks) {
            //can we replace the parent with this lock?
            if (LockType.substitutable(l.lockType, pLock)) {
                return true;
            }
        }
        //no matches
        return false;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("Context is readonly.");
        }
        if (getExplicitLockType(transaction) == LockType.NL) {
            throw new NoLockHeldException("There is no lock 'name' on this transaction");
        }
        if (getNumChildren(transaction) != 0) {
            throw new InvalidLockException("Lock cannot be released b/c on multigran restrictions");
        }

        lockman.release(transaction, name);

        //decrement child
        decChildLocks(transaction, parent, 1);

        return;
    }


    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        //A - > B : TL -> newLockType
        LockType TL = getExplicitLockType(transaction);

        if (readonly) {
            throw new UnsupportedOperationException("Context is readonly.");
        }
        if (TL == newLockType) {
            throw new DuplicateLockRequestException("There is no lock 'name' on this transaction");
        }
        if (TL == LockType.NL) {
            throw new NoLockHeldException("Transaction has no lock.");
        }
        if (!LockType.substitutable(newLockType, TL) || TL == newLockType) {
            if (newLockType != LockType.SIX || !(TL == LockType.IS || TL == LockType.IX || TL == LockType.S)) {
                throw new InvalidLockException("Lock cannot be promoted.");
            }
        }

        //simultaneous release case
        if (newLockType == LockType.SIX && (TL == LockType.IS || TL == LockType.IX)) {
            List<ResourceName> locksToRelease = sisDescendants(transaction);
            locksToRelease.add(name);

            //simultaneous atomic release
            lockman.acquireAndRelease(transaction, name, newLockType, locksToRelease);

            //decrement number of locks released
            decChildLocks(transaction, this, locksToRelease.size());
            //add acquired lock
            incChildLocks(transaction, this, 1);

        }
        else {
            lockman.promote(transaction, name, newLockType);
        }

        return;
    }

    //Helper functions to update number of child locks for each LockContext in transaction

    private void decChildLocks(TransactionContext transaction, LockContext parent, int size) {
        //end if we reached root
        if (parent == null) {
            return;
        }

        Integer numLocks = parent.getNumChildren(transaction);

        numLocks -= size;

        parent.numChildLocks.put(transaction.getTransNum(), numLocks);
        decChildLocks(transaction, parent.parent, size);
    }

    private void incChildLocks(TransactionContext transaction, LockContext parent, int size) {
        //end if we reached root
        if (parent == null) {
            return;
        }

        Integer numLocks = parent.getNumChildren(transaction);

        numLocks += size;

        parent.numChildLocks.put(transaction.getTransNum(), numLocks);
        incChildLocks(transaction, parent.parent, size);
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("Context is readonly.");
        }
        if (getExplicitLockType(transaction) == LockType.NL) {
            throw new NoLockHeldException("Transaction has no lock.");
        }

        //get names of children that we will release
        List<ResourceName> childNames = new ArrayList<>();

        for (Lock l : lockman.getLocks(transaction)) {
            ResourceName potentialChild = l.name;
            if (potentialChild.isDescendantOf(name)) {
                childNames.add(potentialChild);
            }
        }
        childNames.add(name);

        LockType TL = getExplicitLockType(transaction);

        if (TL == LockType.IS) {
            lockman.acquireAndRelease(transaction, name, LockType.S, childNames);
        }
        else if (TL == LockType.IX || TL == LockType.SIX){
            lockman.acquireAndRelease(transaction, name, LockType.X, childNames);
        }

        //decrease num locks removed
        decChildLocks(transaction, this, childNames.size());
        //increment by 1
        incChildLocks(transaction, this, 1);


        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockContext context = parent;
        LockType currLockType = getExplicitLockType(transaction);

        //if its not a NL, effective type is same as explicit
        if (currLockType != LockType.NL) {
            return currLockType;
        }

        //keep going up tree until we hit a non-NL lock
        while (currLockType == LockType.NL && context != null) {
            //recurse through three
            currLockType = context.getEffectiveLockType(transaction);
            context = context.parent;
        }

        //is an S, X lock (thus, NL -> S or X effective type)
        if (!currLockType.isIntent()) {
            return currLockType;
        }
        //if SIX, we can read (S lock)
        else if (currLockType == LockType.SIX) {
            return LockType.S;
        }

        else { // no S or X lock
            return LockType.NL;
        }

    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext context = parent;
        LockType parentLock = LockType.NL;

        while (context != null) {
            parentLock = getExplicitLockType(transaction);
            if (parentLock == LockType.SIX) {
                return true;
            }
            context = context.parent;
        }

        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> sisList = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock l : locks) {
            //Is it an S or IS Lock?
            if (l.lockType == LockType.S || l.lockType == LockType.IS) {
                //descendant of current transaction?
                ResourceName potentialDesc = l.name;
                if (potentialDesc.isDescendantOf(name)) {
                    sisList.add(potentialDesc);
                }
            }
        }
        return sisList;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

