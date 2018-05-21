package com.patloew.rxwear;

import android.net.Uri;
import android.support.annotation.NonNull;

import com.google.android.gms.wearable.Asset;
import com.google.android.gms.wearable.DataMap;
import com.google.android.gms.wearable.MessageEvent;
import com.google.android.gms.wearable.PutDataRequest;

import java.io.Serializable;
import java.util.ArrayList;

import io.reactivex.Observable;
import io.reactivex.Single;

/* Copyright 2016 Patrick Löwenstein
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * FILE MODIFIED by Marek Wałach, 2018
 *
 *
 */
public class Message {

    private final RxWear rxWear;

    Message(RxWear rxWear) {
        this.rxWear = rxWear;
    }

    Uri.Builder getUriBuilder() {
        return new Uri.Builder();
    }

    // listen

    public Observable<MessageEvent> listen() {
        return listenInternal(null, null);
    }

    public Observable<MessageEvent> listen(@NonNull Uri uri, int filterType) {
        return listenInternal(uri, filterType);
    }

    public Observable<MessageEvent> listen(@NonNull String path, int filterType) {
        return listenInternal(getUriBuilder().scheme(PutDataRequest.WEAR_URI_SCHEME).path(path).build(), filterType);
    }

    private Observable<MessageEvent> listenInternal(Uri uri, Integer filterType) {
        return Observable.create(new MessageListenerObservable(rxWear.context, uri, filterType));
    }

    // send

    public Single<Integer> send(@NonNull String nodeId, @NonNull String path, @NonNull byte[] data) {
        return sendInternal(nodeId, path, data);
    }

    Single<Integer> sendInternal(String nodeId, String path, byte[] data) {
        return Single.create(new MessageSendSingle(rxWear.context, nodeId, path, data));
    }

    // sendToAllRemoteNodes

    public Observable<Integer> sendToAllRemoteNodes(@NonNull final String path, @NonNull final byte[] data) {
        return sendToAllRemoteNodesInternal(path, data);
    }

    Observable<Integer> sendToAllRemoteNodesInternal(final String path, final byte[] data) {
        return rxWear.node().getConnectedNodesInternal()
                .flatMap(node -> sendInternal(node.getId(), path, data).toObservable());
    }

    // Helper Methods

    public SendDataMap sendDataMap(String nodeId, String path) {
        return new SendDataMap(nodeId, path, false);
    }

    public SendDataMap sendDataMapToAllRemoteNodes(String path) {
        return new SendDataMap(null, path, true);
    }

    public Single<Integer> sendSerializable(String nodeId, String path, Serializable serializable) {
        try {
            return sendInternal(nodeId, path, IOUtil.writeObjectToByteArray(serializable));
        } catch (Throwable throwable) {
            return Single.error(throwable);
        }
    }

    public Observable<Integer> sendSerializableToAllRemoteNodes(String path, Serializable serializable) {
        try {
            return sendToAllRemoteNodesInternal(path, IOUtil.writeObjectToByteArray(serializable));
        } catch (Throwable throwable) {
            return Observable.error(throwable);
        }
    }

    /* A helper class with a fluent interface for sending a DataMap.
     *
     * Example:
     * rxWear.message().sendDataMap().to(nodeId, "/path")
     *      .putString("key", "value")
     *      .putInt("key", 0)
     *      .toSingle()
     *      .subscribe(requestId -> {
     *          // do something
     *      });
     */
    public class SendDataMap {
        final String nodeId;
        final String path;
        final DataMap dataMap = new DataMap();
        final boolean toAllRemoteNodes;

        private SendDataMap(String nodeId, String path, boolean toAllRemoteNodes) {
            this.nodeId = nodeId;
            this.path = path;
            this.toAllRemoteNodes = toAllRemoteNodes;
        }


        public SendDataMap putAll(DataMap dataMap) {
            dataMap.putAll(dataMap);
            return this;
        }

        public SendDataMap putBoolean(String key, boolean value) {
            dataMap.putBoolean(key, value);
            return this;
        }

        public SendDataMap putByte(String key, byte value) {
            dataMap.putByte(key, value);
            return this;
        }

        public SendDataMap putInt(String key, int value) {
            dataMap.putInt(key, value);
            return this;
        }

        public SendDataMap putLong(String key, long value) {
            dataMap.putLong(key, value);
            return this;
        }

        public SendDataMap putFloat(String key, float value) {
            dataMap.putFloat(key, value);
            return this;
        }

        public SendDataMap putDouble(String key, double value) {
            dataMap.putDouble(key, value);
            return this;
        }

        public SendDataMap putString(String key, String value) {
            dataMap.putString(key, value);
            return this;
        }

        public SendDataMap putAsset(String key, Asset value) {
            dataMap.putAsset(key, value);
            return this;
        }

        public SendDataMap putDataMap(String key, DataMap value) {
            dataMap.putDataMap(key, value);
            return this;
        }

        public SendDataMap putDataMapArrayList(String key, ArrayList<DataMap> value) {
            dataMap.putDataMapArrayList(key, value);
            return this;
        }

        public SendDataMap putIntegerArrayList(String key, ArrayList<Integer> value) {
            dataMap.putIntegerArrayList(key, value);
            return this;
        }

        public SendDataMap putStringArrayList(String key, ArrayList<String> value) {
            dataMap.putStringArrayList(key, value);
            return this;
        }

        public SendDataMap putByteArray(String key, byte[] value) {
            dataMap.putByteArray(key, value);
            return this;
        }

        public SendDataMap putLongArray(String key, long[] value) {
            dataMap.putLongArray(key, value);
            return this;
        }

        public SendDataMap putFloatArray(String key, float[] value) {
            dataMap.putFloatArray(key, value);
            return this;
        }

        public SendDataMap putStringArray(String key, String[] value) {
            dataMap.putStringArray(key, value);
            return this;
        }

        public Observable<Integer> toObservable() {
            if (toAllRemoteNodes) {
                return sendToAllRemoteNodesInternal(path, dataMap.toByteArray());
            } else {
                return sendInternal(nodeId, path, dataMap.toByteArray()).toObservable();
            }
        }

        /* This should only be used with to(). If used with
         * toAllRemoteNodes(), an Exception will be thrown
         * if more than one item is emitted (i.e. more than one
         * node is connected).
         */
        public Single<Integer> toSingle() {
            if (toAllRemoteNodes) {
                return Single.error(new UnsupportedOperationException("toSingle() can not be used with toAllRemoteNodes()"));
            } else {
                return sendInternal(nodeId, path, dataMap.toByteArray());
            }
        }
    }

}
