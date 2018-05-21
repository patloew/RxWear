package com.patloew.rxwear;

import android.content.Context;
import android.net.Uri;
import android.support.annotation.NonNull;

import com.google.android.gms.wearable.Asset;
import com.google.android.gms.wearable.DataClient;
import com.google.android.gms.wearable.DataEvent;
import com.google.android.gms.wearable.DataItem;
import com.google.android.gms.wearable.DataItemAsset;
import com.google.android.gms.wearable.DataMap;
import com.google.android.gms.wearable.DataMapItem;
import com.google.android.gms.wearable.PutDataMapRequest;
import com.google.android.gms.wearable.PutDataRequest;

import java.io.IOException;
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
public class Data {

    private final Context context;

    Data(Context context) {
        this.context = context;
    }


    Uri.Builder getUriBuilder() {
        return new Uri.Builder();
    }

    // listen

    public Observable<DataEvent> listen() {
        return listenInternal(null, null);
    }

    public Observable<DataEvent> listen(@NonNull Uri uri, int filterType) {
        return listenInternal(uri, filterType);
    }

    public Observable<DataEvent> listen(@NonNull String path, int filterType) {
        return listenInternal(getUriBuilder().scheme(PutDataRequest.WEAR_URI_SCHEME).path(path).build(), filterType);
    }

    private Observable<DataEvent> listenInternal(Uri uri, Integer filterType) {
        return Observable.create(new DataListenerObservable(context, uri, filterType));
    }

    // delete

    public Single<Integer> delete(@NonNull Uri uri) {
        return deleteInternal(uri, null);
    }

    public Single<Integer> delete(@NonNull Uri uri, int filterType) {
        return deleteInternal(uri, filterType);
    }

    private Single<Integer> deleteInternal(Uri uri, Integer filterType) {
        return Single.create(new DataDeleteItemsSingle(context, uri, filterType));
    }

    // put

    public Single<DataItem> put(@NonNull PutDataRequest putDataRequest) {
        return putInternal(putDataRequest);
    }

    public Single<DataItem> put(@NonNull PutDataMapRequest putDataMapRequest) {
        return putInternal(putDataMapRequest.asPutDataRequest());
    }

    Single<DataItem> putInternal(PutDataRequest putDataRequest) {
        return Single.create(new DataPutItemSingle(context, putDataRequest));
    }

    // get

    public Observable<DataItem> get(@NonNull Uri uri, int filterType) {
        return getInternal(uri, filterType);
    }

    public Observable<DataItem> get(@NonNull String path, int filterType) {
        return getInternal(getUriBuilder().scheme(PutDataRequest.WEAR_URI_SCHEME).path(path).build(), filterType);
    }

    public Observable<DataItem> get(@NonNull Uri uri) {
        return getInternal(uri, null);
    }

    public Observable<DataItem> get(@NonNull String path) {
        return getInternal(getUriBuilder().scheme(PutDataRequest.WEAR_URI_SCHEME).path(path).build(), null);
    }

    public Observable<DataItem> get() {
        return getInternal(null, null);
    }

    private Observable<DataItem> getInternal(Uri uri, Integer filterType) {
        return Observable.create(new DataGetItemsObservable(context, uri, filterType));
    }

    // getFdForAsset

    public Single<DataClient.GetFdForAssetResponse> getFdForAsset(@NonNull DataItemAsset dataItemAsset) {
        return getFdForAssetInternal(dataItemAsset, null);
    }

    public Single<DataClient.GetFdForAssetResponse> getFdForAsset(@NonNull Asset asset) {
        return getFdForAssetInternal(null, asset);
    }

    private Single<DataClient.GetFdForAssetResponse> getFdForAssetInternal(DataItemAsset dataItemAsset, Asset asset) {
        return Single.create(new DataGetFdForAssetSingle(context, dataItemAsset, asset));
    }


    // Helper

    public PutDataMap putDataMap() {
        return new PutDataMap();
    }

    public PutSerializable putSerializable(Serializable serializable) {
        return new PutSerializable(serializable);
    }


    /* A helper class for putting a Serializable.
     *
     * Example:
     * rxWear.data().putSerializable(serializable).urgent().to("/path")
     *      .subscribe(dataItem -> {
     *          // do something
     *      });
     */
    public class PutSerializable {

        final Serializable serializable;
        boolean urgent = false;

        PutSerializable(Serializable serializable) {
            this.serializable = serializable;
        }

        public PutSerializable urgent() {
            urgent = true;
            return this;
        }

        public Single<DataItem> withDataItem(DataItem dataItem) {
            PutDataRequest request = PutDataRequest.createFromDataItem(dataItem);
            if (urgent) {
                request.setUrgent();
            }
            return createPutSerializableSingle(request);
        }

        public Single<DataItem> withAutoAppendedId(String pathPrefix) {
            PutDataRequest request = PutDataRequest.createWithAutoAppendedId(pathPrefix);
            if (urgent) {
                request.setUrgent();
            }
            return createPutSerializableSingle(request);
        }

        public Single<DataItem> to(String path) {
            PutDataRequest request = PutDataRequest.create(path);
            if (urgent) {
                request.setUrgent();
            }
            return createPutSerializableSingle(request);
        }

        private Single<DataItem> createPutSerializableSingle(PutDataRequest request) {
            try {
                request.setData(IOUtil.writeObjectToByteArray(serializable));
                return putInternal(request);
            } catch (IOException e) {
                return Single.error(e);
            }
        }
    }

    /* A helper class with a fluent interface for putting a DataItem
     * based on a DataMap.
     *
     * Example:
     * rxWear.data().putDataMap().urgent().to("/path")
     *      .putString("key", "value")
     *      .putInt("key", 0)
     *      .toSingle()
     *      .subscribe(dataItem -> {
     *          // do something
     *      });
     */
    public class PutDataMap {

        PutDataMap() {
        }

        boolean urgent = false;

        public PutDataMap urgent() {
            urgent = true;
            return this;
        }

        public RxFitPutDataMapRequest withDataMapItem(DataMapItem source) {
            return new RxFitPutDataMapRequest(null, source, null, urgent);
        }

        public RxFitPutDataMapRequest withAutoAppendedId(String pathPrefix) {
            return new RxFitPutDataMapRequest(null, null, pathPrefix, urgent);
        }

        public RxFitPutDataMapRequest to(String path) {
            return new RxFitPutDataMapRequest(path, null, null, urgent);
        }
    }


    public class RxFitPutDataMapRequest {
        final PutDataMapRequest request;

        private RxFitPutDataMapRequest(String path, DataMapItem dataMapItem, String pathPrefix, boolean urgent) {
            if (path != null) {
                request = PutDataMapRequest.create(path);
            } else if (dataMapItem != null) {
                request = PutDataMapRequest.createFromDataMapItem(dataMapItem);
            } else {
                request = PutDataMapRequest.createWithAutoAppendedId(pathPrefix);
            }

            if (urgent) {
                request.setUrgent();
            }
        }

        public RxFitPutDataMapRequest putAll(DataMap dataMap) {
            request.getDataMap().putAll(dataMap);
            return this;
        }

        public RxFitPutDataMapRequest putBoolean(String key, boolean value) {
            request.getDataMap().putBoolean(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putByte(String key, byte value) {
            request.getDataMap().putByte(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putInt(String key, int value) {
            request.getDataMap().putInt(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putLong(String key, long value) {
            request.getDataMap().putLong(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putFloat(String key, float value) {
            request.getDataMap().putFloat(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putDouble(String key, double value) {
            request.getDataMap().putDouble(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putString(String key, String value) {
            request.getDataMap().putString(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putAsset(String key, Asset value) {
            request.getDataMap().putAsset(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putDataMap(String key, DataMap value) {
            request.getDataMap().putDataMap(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putDataMapArrayList(String key, ArrayList<DataMap> value) {
            request.getDataMap().putDataMapArrayList(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putIntegerArrayList(String key, ArrayList<Integer> value) {
            request.getDataMap().putIntegerArrayList(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putStringArrayList(String key, ArrayList<String> value) {
            request.getDataMap().putStringArrayList(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putByteArray(String key, byte[] value) {
            request.getDataMap().putByteArray(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putLongArray(String key, long[] value) {
            request.getDataMap().putLongArray(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putFloatArray(String key, float[] value) {
            request.getDataMap().putFloatArray(key, value);
            return this;
        }

        public RxFitPutDataMapRequest putStringArray(String key, String[] value) {
            request.getDataMap().putStringArray(key, value);
            return this;
        }

        public Single<DataItem> toSingle() {
            return putInternal(request.asPutDataRequest());
        }

        public Observable<DataItem> toObservable() {
            return putInternal(request.asPutDataRequest()).toObservable();
        }
    }

}
