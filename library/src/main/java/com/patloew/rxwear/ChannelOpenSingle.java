package com.patloew.rxwear;

import android.content.Context;

import com.google.android.gms.wearable.ChannelClient;
import com.google.android.gms.wearable.Wearable;

import java.io.IOException;

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
class ChannelOpenSingle extends BaseSingle<ChannelClient.Channel> {

    final String nodeId;
    final String path;

    ChannelOpenSingle(Context context, String nodeId, String path) {
        super(context);
        this.nodeId = nodeId;
        this.path = path;
    }

    @Override
    void onSubscribe(SingleEmitter<ChannelClient.Channel> channelSingleEmitter) {
        setupWearTask(Wearable.getChannelClient(context).openChannel(nodeId, path),
                openChannelResult -> {
                    if (openChannelResult.isSuccessful()) {
                        if (openChannelResult.getResult() != null) {
                            channelSingleEmitter.onSuccess(openChannelResult.getResult());
                        } else {
                            channelSingleEmitter.onError(new IOException("Channel connection could not be opened"));
                        }
                    } else {
                        channelSingleEmitter.onError(new StatusException(openChannelResult.getException()));
                    }
                });
    }
}
