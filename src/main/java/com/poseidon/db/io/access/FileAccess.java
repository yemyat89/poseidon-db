package com.poseidon.db.io.access;

import java.io.IOException;
import java.util.List;
import com.poseidon.db.representation.DataItem;
import com.poseidon.db.utils.Pair;

public interface FileAccess {
	public DataItem getValue(DataItem key, long startOffset, long limitOffset) throws IOException;

	public Pair<DataItem, List<Pair<DataItem, DataItem>>> getValueAndLoadBlock(DataItem key, long startOffset,
			long limitOffset) throws IOException;
}
