/*
 * KeyValueStoreRocksDB.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "IKeyValueStore.h"
#include "CoroFlow.h"
#include "Knobs.h"
#include "flow/Hash3.h"


#include "flow/ThreadPrimitives.h"
#include "template_fdb.h"
#include "fdbrpc/simulator.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/threadpool.h"
#include "rocksdb/convenience.h"
#include "flow/actorcompiler.h"  // This must be the last #include.


struct WriteTransaction : FastAllocated<WriteTransaction> {
	rocksdb::WriteBatch writeBatch;
	rocksdb::WriteOptions writeOptions;
	ThreadReturnPromise<Void> result;

	WriteTransaction() {
		writeOptions.disableWAL = false;
		writeOptions.sync = true;
	}
};

struct ReadTransaction : FastAllocated<ReadTransaction> {
	Key key;
	Optional<UID> debugID;
	rocksdb::ReadOptions readOptions;
	ThreadReturnPromise<Optional<Value>> result;

	ReadTransaction(KeyRef key, Optional<UID> debugID) : key(key), debugID(debugID) {}
};

struct ReadPartTransaction : FastAllocated<ReadPartTransaction> {
	Key key;
	int maxLength;
	Optional<UID> debugID;
	rocksdb::ReadOptions readOptions;
	ThreadReturnPromise<Optional<Value>> result;

	ReadPartTransaction(KeyRef key, int maxLength, Optional<UID> debugID) : key(key), maxLength(maxLength), debugID(debugID) {}
};

struct ReadRangeTransaction : FastAllocated<ReadRangeTransaction> {
	KeyRange keys;
	int rowLimit;
	int byteLimit;
	rocksdb::ReadOptions readOptions;
	ThreadReturnPromise<Standalone<VectorRef<KeyValueRef>>> result;

	ReadRangeTransaction(KeyRangeRef keys, int rowLimit, int byteLimit) : keys(keys), rowLimit(rowLimit), byteLimit(byteLimit) {}
};


class KeyValueStoreRocksDB : public IKeyValueStore {
public:
	virtual void dispose() { doClose(this, true); }
	virtual void close() { doClose(this, false); }

	virtual Future<Void> getError() { return errors.getFuture(); }
	virtual Future<Void> onClosed() { return stopped.getFuture(); }

	virtual KeyValueStoreType getType() { return type; }
	virtual StorageBytes getStorageBytes();

	virtual void set( KeyValueRef keyValue, const Arena* arena = NULL );
	virtual void clear( KeyRangeRef range, const Arena* arena = NULL );
	virtual Future<Void> commit(bool sequential = false);

	virtual Future<Optional<Value>> readValue( KeyRef key, Optional<UID> debugID );
	virtual Future<Optional<Value>> readValuePrefix( KeyRef key, int maxLength, Optional<UID> debugID );
	virtual Future<Standalone<VectorRef<KeyValueRef>>> readRange( KeyRangeRef keys, int rowLimit = 1<<30, int byteLimit = 1<<30 );

	KeyValueStoreRocksDB(std::string const& path, UID logID, KeyValueStoreType type, std::string const& options = "");
	~KeyValueStoreRocksDB();

private:
	std::string path;
	UID logID;
	KeyValueStoreType type;

	rocksdb::ThreadPool *threads;
	Promise<Void> errors, stopped;
	Future<Void> stopOnErr;
	int64_t readsRequested, writesRequested;
	WriteTransaction *currentWriteTransaction;
	rocksdb::Options options;
	rocksdb::DB *db;

	ACTOR static Future<Void> stopOnError( KeyValueStoreRocksDB* self ) {
		try {
			wait( self->errors.getFuture() );
			TraceEvent("KeyValueStoreRocksDB").detail("stop on error after wait", "");
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				TraceEvent("KeyValueStoreRocksDB").detail("stop on error actor cacelled", "");
				throw;
			}
		}

		return Void();
	}

	ACTOR static void doClose( KeyValueStoreRocksDB* self, bool deleteOnClose ) {
		self->threads->JoinAllThreads();
		if (deleteOnClose) {
			//do something
		}
		self->stopped.send(Void());
	}
};

IKeyValueStore* keyValueStoreRocksDB( std::string const& path, UID logID, KeyValueStoreType storeType, std::string const& options_str) {
	std::string options = "compression=kNoCompression;max_write_buffer_number=4;min_write_buffer_number_to_merge=1;recycle_log_file_num=4;write_buffer_size=268435456;writable_file_max_buffer_size=0;compaction_readahead_size=2097152;disableWAL=true;";
	return new KeyValueStoreRocksDB(path, logID, storeType, options);
}

KeyValueStoreRocksDB::KeyValueStoreRocksDB(std::string const& path, UID id, KeyValueStoreType storeType, std::string const& options_str)
	: path(path),logID(id),type(storeType),
	  readsRequested(0), writesRequested(0)
{
	stopOnErr = stopOnError(this);

	int taskId = g_network->getCurrentTask();
	g_network->setCurrentTask(TaskDiskWrite);

	threads = rocksdb::NewThreadPool(4);

	rocksdb::GetDBOptionsFromString(options, options_str, &options);
	options.create_if_missing = true;
	rocksdb::Status s=rocksdb::DB::Open(options, path, &db);
	ASSERT(s.ok());

	currentWriteTransaction = new WriteTransaction();

	g_network->setCurrentTask(taskId);
	TraceEvent("KeyValueStoreRocksDB").detail("construction ok", "");
}

KeyValueStoreRocksDB::~KeyValueStoreRocksDB() {
	delete threads;
	delete currentWriteTransaction;
	delete db;
}

StorageBytes KeyValueStoreRocksDB::getStorageBytes() {
	int64_t free;
	int64_t total;

	g_network->getDiskBytes(parentDirectory(path), free, total);

	return StorageBytes(free, total, total - free, free * 0.95);
}

void KeyValueStoreRocksDB::set( KeyValueRef keyValue, const Arena* arena ) {
	++writesRequested;
	currentWriteTransaction->writeBatch.Put(rocksdb::Slice((const char*)keyValue.key.begin(), keyValue.key.size()), rocksdb::Slice((const char*)keyValue.value.begin(), keyValue.value.size()));
}

void KeyValueStoreRocksDB::clear( KeyRangeRef range, const Arena* arena ) {
	++writesRequested;
	currentWriteTransaction->writeBatch.DeleteRange(rocksdb::Slice((const char*)range.begin.begin(), range.begin.size()), rocksdb::Slice((const char*)range.end.begin(), range.end.size()));
}

Future<Void> KeyValueStoreRocksDB::commit(bool sequential) {
	++writesRequested;

	auto writeTransaction = currentWriteTransaction;
	auto f = writeTransaction->result.getFuture();
	currentWriteTransaction = new WriteTransaction();

	threads->SubmitJob([this, writeTransaction] {
		rocksdb::Status s = db->Write(writeTransaction->writeOptions, &writeTransaction->writeBatch);
		if (!s.ok()) {
			TraceEvent("KeyValueStoreRocksDB").detail("write is not ok", s.ToString());
			std::string temp = s.ToString();
			printf("commit ret=%s\n", temp.c_str());
			errors.sendError( io_error() );
			throw io_error();
		} else {
			writeTransaction->result.send(Void());
		}
		delete writeTransaction;
	});

	return f;
}

Future<Optional<Value>> KeyValueStoreRocksDB::readValue( KeyRef key, Optional<UID> debugID ) {
	++readsRequested;
	auto readTransaction  = new ReadTransaction(key, debugID);
	auto f = readTransaction->result.getFuture();

	threads->SubmitJob([this, readTransaction] {
		std::string value;
		rocksdb::Status s = db->Get(readTransaction->readOptions, rocksdb::Slice((const char*)readTransaction->key.begin(), readTransaction->key.size()), &value);
		if (s.IsNotFound()) {
			readTransaction->result.send(Optional<Value>());
		} else if (!s.ok()) {
			TraceEvent("KeyValueStoreRocksDB").detail("read is not ok", s.ToString());
			errors.sendError( io_error() );
			throw io_error();
		} else {
			ValueRef result((const uint8_t*)value.data(), value.length());
			readTransaction->result.send(Value(result));
		}
		delete readTransaction;
	});

	return f;
}

Future<Optional<Value>> KeyValueStoreRocksDB::readValuePrefix( KeyRef key, int maxLength, Optional<UID> debugID ) {
	++readsRequested;
	auto readPartTransaction  = new ReadPartTransaction(key, maxLength, debugID);
	auto f = readPartTransaction->result.getFuture();

	threads->SubmitJob([this, readPartTransaction] {
		std::string value;
		rocksdb::Status s = db->Get(readPartTransaction->readOptions, rocksdb::Slice((const char*)readPartTransaction->key.begin(), readPartTransaction->key.size()), &value);
		if (s.IsNotFound()) {
			readPartTransaction->result.send(Optional<Value>());
		} else if (!s.ok()) {
			TraceEvent("KeyValueStoreRocksDB").detail("read prefix is not ok", s.ToString());
			errors.sendError( io_error() );
			throw io_error();
		} else {
			ValueRef result((const uint8_t*)value.data(), readPartTransaction->maxLength < value.length() ? readPartTransaction->maxLength : value.length());
			readPartTransaction->result.send(Value(result));
		}
		delete readPartTransaction;
	});

	return f;
}

Future<Standalone<VectorRef<KeyValueRef>>> KeyValueStoreRocksDB::readRange( KeyRangeRef keys, int rowLimit, int byteLimit ) {
	++readsRequested;
	auto readRangeTransaction = new ReadRangeTransaction(keys, rowLimit, byteLimit);
	auto f = readRangeTransaction->result.getFuture();

	threads->SubmitJob([this, readRangeTransaction] {
		rocksdb::Iterator *itr = db->NewIterator(readRangeTransaction->readOptions);
		Standalone<VectorRef<KeyValueRef>> result;
		int bytes = 0;
		int rowLimit = readRangeTransaction->rowLimit;
		if (rowLimit >= 0) {
			for (itr->Seek(rocksdb::Slice((const char*)readRangeTransaction->keys.begin.begin(), readRangeTransaction->keys.begin.size()));
				rowLimit-- && bytes < readRangeTransaction->byteLimit &&
				itr->Valid() && KeyRef((const uint8_t*)itr->key().data(), itr->key().size()) < readRangeTransaction->keys.end;
				itr->Next()) {
				KeyValueRef kv = KeyValueRef(KeyRef((const uint8_t*)itr->key().data(), itr->key().size()), ValueRef((const uint8_t*)itr->value().data(), itr->value().size()));
				result.push_back_deep(result.arena(), kv);
				bytes += sizeof(KeyValueRef) + kv.expectedSize();
			}
		} else {
			for (itr->SeekForPrev(rocksdb::Slice((const char*)readRangeTransaction->keys.end.begin(), readRangeTransaction->keys.end.size()));
				rowLimit++ && bytes < readRangeTransaction->byteLimit &&
				itr->Valid() && KeyRef((const uint8_t*)itr->key().data(), itr->key().size()) >= readRangeTransaction->keys.begin;
                                itr->Prev()) {
                                KeyValueRef kv = KeyValueRef(KeyRef((const uint8_t*)itr->key().data(), itr->key().size()), ValueRef((const uint8_t*)itr->value().data(), itr->value().size()));
                                result.push_back_deep(result.arena(), kv);
                                bytes += sizeof(KeyValueRef) + kv.expectedSize();
			}
		}
		readRangeTransaction->result.send(result);
		delete itr;
		delete readRangeTransaction;
	});

	return f;
}
