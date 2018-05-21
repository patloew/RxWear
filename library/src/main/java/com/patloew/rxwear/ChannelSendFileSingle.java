package com.patloew.rxwear;

import android.content.Context;
import android.net.Uri;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.wearable.ChannelClient;
import com.google.android.gms.wearable.Wearable;

import io.reactivex.SingleEmitter;
import io.reactivex.annotations.NonNull;

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
class ChannelSendFileSingle extends BaseSingle<Void> {

    final ChannelClient.Channel channel;
    final Uri uri;
    final Long startOffset;
    final Long length;

    ChannelSendFileSingle(Context context, @NonNull ChannelClient.Channel channel, Uri uri, Long startOffset, Long length) {
        super(context);
        this.channel = channel;
        this.uri = uri;
        this.startOffset = startOffset;
        this.length = length;
    }

    @Override
    void onSubscribe(SingleEmitter<Void> voidSingleEmitter) {
        OnCompleteListener<Void> resultCallBack = SingleResultCallBack.get(voidSingleEmitter);

        if (startOffset != null && length != null) {
            setupWearTask(Wearable.getChannelClient(context).sendFile(channel, uri, startOffset, length), resultCallBack);
        } else {
            setupWearTask(Wearable.getChannelClient(context).sendFile(channel, uri), resultCallBack);
        }
    }
}
