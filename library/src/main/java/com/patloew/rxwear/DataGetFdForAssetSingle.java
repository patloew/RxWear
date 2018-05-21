package com.patloew.rxwear;

import android.content.Context;
import android.support.annotation.NonNull;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.wearable.Asset;
import com.google.android.gms.wearable.DataClient;
import com.google.android.gms.wearable.DataItemAsset;
import com.google.android.gms.wearable.Wearable;

import io.reactivex.SingleEmitter;

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
class DataGetFdForAssetSingle extends BaseSingle<DataClient.GetFdForAssetResponse> {

    final DataItemAsset dataItemAsset;
    final Asset asset;

    DataGetFdForAssetSingle(@NonNull Context ctx, DataItemAsset dataItemAsset, Asset asset) {
        super(ctx);
        this.dataItemAsset = dataItemAsset;
        this.asset = asset;
    }

    @Override
    void onSubscribe(SingleEmitter<DataClient.GetFdForAssetResponse> getFdForAssetResponseSingleEmitter) {
        OnCompleteListener<DataClient.GetFdForAssetResponse> resultCallback = SingleResultCallBack.get(getFdForAssetResponseSingleEmitter);

        if (asset != null) {
            setupWearTask(Wearable.getDataClient(context).getFdForAsset(asset), resultCallback);
        } else {
            setupWearTask(Wearable.getDataClient(context).getFdForAsset(dataItemAsset), resultCallback);
        }
    }
}
