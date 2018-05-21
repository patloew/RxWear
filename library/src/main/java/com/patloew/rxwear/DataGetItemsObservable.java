package com.patloew.rxwear;

import android.content.Context;
import android.net.Uri;
import android.support.annotation.NonNull;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.wearable.DataItem;
import com.google.android.gms.wearable.DataItemBuffer;
import com.google.android.gms.wearable.Wearable;

import io.reactivex.ObservableEmitter;

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
class DataGetItemsObservable extends BaseObservable<DataItem> {

    final Uri uri;
    final Integer filterType;

    DataGetItemsObservable(@NonNull Context context, Uri uri, Integer filterType) {
        super(context);
        this.uri = uri;
        this.filterType = filterType;
    }

    @Override
    void onSubscribe(ObservableEmitter<DataItem> dataItemObservableEmitter) {
        OnCompleteListener<DataItemBuffer> resultCallback = dataItemBuffer -> {
            try {
                if (!dataItemBuffer.isSuccessful()) {
                    dataItemObservableEmitter.onError(new StatusException(dataItemBuffer.getException()));
                } else {
                    for (int i = 0; i < dataItemBuffer.getResult().getCount(); i++) {
                        if (dataItemObservableEmitter.isDisposed()) {
                            break;
                        }
                        dataItemObservableEmitter.onNext(dataItemBuffer.getResult().get(i).freeze());
                    }

                    dataItemObservableEmitter.onComplete();
                }
            } catch (Throwable throwable) {
                dataItemObservableEmitter.onError(throwable);
            }
        };

        if (uri == null) {
            setupWearTask(Wearable.getDataClient(context).getDataItems(), resultCallback);
        } else {
            if (filterType == null) {
                setupWearTask(Wearable.getDataClient(context).getDataItems(uri), resultCallback);
            } else {
                setupWearTask(Wearable.getDataClient(context).getDataItems(uri, filterType), resultCallback);
            }
        }
    }
}
