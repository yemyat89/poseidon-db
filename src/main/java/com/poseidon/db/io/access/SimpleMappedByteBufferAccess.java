package com.poseidon.db.io.access;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import com.poseidon.db.representation.DataItem;
import com.poseidon.db.utils.DataConversion;
import com.poseidon.db.utils.Pair;

public class SimpleMappedByteBufferAccess implements FileAccess {

	private RandomAccessFile sstableFile;
	private MappedByteBuffer mappedBuffer;

	public SimpleMappedByteBufferAccess(RandomAccessFile sstableFile) throws IOException {
		this.sstableFile = sstableFile;
		mappedBuffer = this.sstableFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.sstableFile.length());
	}

	@Override
	public DataItem getValue(DataItem key, long startOffset, long limitOffset) throws IOException {

		// TODO: Incorrect 32 bits rounding, i.e. not more than 2GB
		int currentPosition = (int) startOffset;
		mappedBuffer.position(currentPosition);

		while (true) {

			byte[] bufRecordLength = new byte[4];
			byte[] bufKeyLength = new byte[4];

			mappedBuffer.get(bufRecordLength);
			mappedBuffer.get(bufKeyLength);

			byte[] keyData = new byte[DataConversion.byteArrayToInt(bufKeyLength)];
			byte[] valueData = new byte[DataConversion.byteArrayToInt(bufRecordLength)
					- DataConversion.byteArrayToInt(bufKeyLength)];

			mappedBuffer.get(keyData);
			mappedBuffer.get(valueData);

			DataItem keyDt = new DataItem(keyData);
			DataItem valueDt = new DataItem(valueData);
			if (keyDt.equals(key)) {
				return valueDt;
			}

			if (keyDt.compareTo(key) > 0 || mappedBuffer.position() >= (int) limitOffset) {
				break;
			}
		}

		return null;
	}

	@Override
	public Pair<DataItem, List<Pair<DataItem, DataItem>>> getValueAndLoadBlock(DataItem key, long startOffset,
			long limitOffset) throws IOException {

		DataItem found = null;
		List<Pair<DataItem, DataItem>> block = new ArrayList<Pair<DataItem, DataItem>>();

		// TODO: Incorrect 32 bits rounding, i.e. not more than 2GB
		int currentPosition = (int) startOffset;
		mappedBuffer.position(currentPosition);

		while (true) {

			byte[] bufRecordLength = new byte[4];
			byte[] bufKeyLength = new byte[4];

			mappedBuffer.get(bufRecordLength);
			mappedBuffer.get(bufKeyLength);
			byte[] keyData = new byte[DataConversion.byteArrayToInt(bufKeyLength)];
			byte[] valueData = new byte[DataConversion.byteArrayToInt(bufRecordLength)
					- DataConversion.byteArrayToInt(bufKeyLength)];

			mappedBuffer.get(keyData);
			mappedBuffer.get(valueData);

			DataItem keyDt = new DataItem(keyData);
			DataItem valueDt = new DataItem(valueData);

			// No need to collect blocks, as Virtual Memory takes care of swap

			if (keyDt.equals(key)) {
				found = valueDt;
				break;
			}

			if (keyDt.compareTo(key) > 0 || mappedBuffer.position() == (int) limitOffset) {
				break;
			}
		}

		return new Pair<DataItem, List<Pair<DataItem, DataItem>>>(found, block);
	}

}
