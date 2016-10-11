package com.poseidon.db.utils;

public class Pair<L,R> {

	  private final L left;
	  private final R right;

	  public Pair(L left, R right) {
	    this.left = left;
	    this.right = right;
	  }

	  public L getLeft() { return left; }
	  public R getRight() { return right; }

	  @Override
	  public int hashCode() { return left.hashCode() ^ right.hashCode(); }

	  @Override
	  @SuppressWarnings("unchecked")
	  public boolean equals(Object other) {
	    if (!(other instanceof Pair)) return false;
	    Pair<L, R> pairOther = (Pair<L, R>) other;
	    return this.left.equals(pairOther.getLeft()) &&
	           this.right.equals(pairOther.getRight());
	  }

	}