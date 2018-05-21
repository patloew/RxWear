package com.patloew.rxwear;

import android.content.Context;
import android.support.annotation.NonNull;

import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.wearable.ChannelClient;
import com.google.android.gms.wearable.Wearable;
import com.patloew.rxwear.events.ChannelClosedEvent;
import com.patloew.rxwear.events.ChannelEvent;
import com.patloew.rxwear.events.ChannelOpenedEvent;
import com.patloew.rxwear.events.InputClosedEvent;
import com.patloew.rxwear.events.OutputClosedEvent;

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
class ChannelListenerObservable extends BaseObservable<ChannelEvent> {

    final ChannelClient.Channel channel;
    private ChannelClient.ChannelCallback channelCallback;

    ChannelListenerObservable(Context context, ChannelClient.Channel channel) {
        super(context);
        this.channel = channel;
    }

    @Override
    void onSubscribe(ObservableEmitter<ChannelEvent> channelEventObservableEmitter) {
        channelCallback = new ChannelClient.ChannelCallback() {
            @Override
            public void onChannelOpened(@NonNull ChannelClient.Channel channel) {
                channelEventObservableEmitter.onNext(new ChannelOpenedEvent(channel));
            }

            @Override
            public void onChannelClosed(@NonNull ChannelClient.Channel channel, int closeReason, int appSpecificErrorCode) {
                channelEventObservableEmitter.onNext(new ChannelClosedEvent(channel, closeReason, appSpecificErrorCode));
            }

            @Override
            public void onInputClosed(@NonNull ChannelClient.Channel channel, int closeReason, int appSpecificErrorCode) {
                channelEventObservableEmitter.onNext(new InputClosedEvent(channel, closeReason, appSpecificErrorCode));
            }

            @Override
            public void onOutputClosed(@NonNull ChannelClient.Channel channel, int closeReason, int appSpecificErrorCode) {
                channelEventObservableEmitter.onNext(new OutputClosedEvent(channel, closeReason, appSpecificErrorCode));
            }
        };

        OnCompleteListener<Void> resultCallback = new StatusErrorResultCallBack<>(channelEventObservableEmitter);

        if (channel != null) {
            setupWearTask(Wearable.getChannelClient(context).registerChannelCallback(channel, channelCallback), resultCallback);
        } else {
            setupWearTask(Wearable.getChannelClient(context).registerChannelCallback(channelCallback), resultCallback);
        }
    }

    @Override
    void unSubscribe() {
        if (channel != null) {
            Wearable.getChannelClient(context).unregisterChannelCallback(channel, channelCallback);
        } else {
            Wearable.getChannelClient(context).unregisterChannelCallback(channelCallback);
        }
    }
}
