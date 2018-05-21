package com.patloew.rxwear;

import android.content.Context;
import android.support.annotation.NonNull;

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
public class RxWear {

    final Context context;

    private final Capability capability;
    private final Channel channel;
    private final Data data;
    private final Message message;
    private final Node node;


    public RxWear(@NonNull Context context) {
        this.context = context.getApplicationContext();

        capability = new Capability(context);
        channel = new Channel(context);
        data = new Data(context);
        message = new Message(this);
        node = new Node(context);
    }

    public Capability capability() {
        return capability;
    }

    public Channel channel() {
        return channel;
    }

    public Data data() {
        return data;
    }

    public Message message() {
        return message;
    }

    public Node node() {
        return node;
    }
}
