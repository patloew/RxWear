package com.patloew.rxwear;

import android.content.Context;
import android.net.Uri;
import android.support.annotation.NonNull;

import com.google.android.gms.wearable.ChannelClient;
import com.patloew.rxwear.events.ChannelEvent;

import java.io.InputStream;
import java.io.OutputStream;

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
public class Channel {

    private final Context context;

    Channel(Context context) {
        this.context = context;
    }

    // listen

    public Observable<ChannelEvent> listen() {
        return listenInternal(null);
    }

    public Observable<ChannelEvent> listen(@NonNull ChannelClient.Channel channel) {
        return listenInternal(channel);
    }

    private Observable<ChannelEvent> listenInternal(ChannelClient.Channel channel) {
        return Observable.create(new ChannelListenerObservable(context, channel));
    }

    // close
    public Single<Void> close(@NonNull ChannelClient.Channel channel) {
        return closeInternal(channel, null);
    }

    public Single<Void> close(@NonNull ChannelClient.Channel channel, int errorCode) {
        return closeInternal(channel, errorCode);
    }

    private Single<Void> closeInternal(ChannelClient.Channel channel, Integer errorCode) {
        return Single.create(new ChannelCloseSingle(context, channel, errorCode));
    }

    // sendFile

    public Single<Void> sendFile(@NonNull ChannelClient.Channel channel, @NonNull Uri uri) {
        return sendFileInternal(channel, uri, null, null);
    }

    public Single<Void> sendFile(@NonNull ChannelClient.Channel channel, @NonNull Uri uri, long startOffset, long length) {
        return sendFileInternal(channel, uri, startOffset, length);
    }

    private Single<Void> sendFileInternal(ChannelClient.Channel channel, Uri uri, Long startOffset, Long length) {
        return Single.create(new ChannelSendFileSingle(context, channel, uri, startOffset, length));
    }

    // receiveFile

    public Single<Void> receiveFile(@NonNull ChannelClient.Channel channel, @NonNull Uri uri, boolean append) {
        return receiveFileInternal(channel, uri, append);
    }

    private Single<Void> receiveFileInternal(ChannelClient.Channel channel, Uri uri, boolean append) {
        return Single.create(new ChannelReceiveFileSingle(context, channel, uri, append));
    }

    // getInputStream

    public Single<InputStream> getInputStream(@NonNull ChannelClient.Channel channel) {
        return getInputStreamInternal(channel);
    }

    private Single<InputStream> getInputStreamInternal(ChannelClient.Channel channel) {
        return Single.create(new ChannelGetInputStreamSingle(context, channel));
    }

    // getOutputStream

    public Single<OutputStream> getOutputStream(@NonNull ChannelClient.Channel channel) {
        return getOutputStreamInternal(channel);
    }

    private Single<OutputStream> getOutputStreamInternal(ChannelClient.Channel channel) {
        return Single.create(new ChannelGetOutputStreamSingle(context, channel));
    }

    // open

    public Single<ChannelClient.Channel> open(@NonNull String nodeId, @NonNull String path) {
        return openInternal(nodeId, path);
    }

    private Single<ChannelClient.Channel> openInternal(String nodeId, String path) {
        return Single.create(new ChannelOpenSingle(context, nodeId, path));
    }
}
