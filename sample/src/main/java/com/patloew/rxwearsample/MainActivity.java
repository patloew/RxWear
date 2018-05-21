package com.patloew.rxwearsample;

import android.content.Context;
import android.os.Bundle;
import android.support.design.widget.CoordinatorLayout;
import android.support.design.widget.Snackbar;
import android.support.v7.app.AppCompatActivity;
import android.text.TextUtils;
import android.util.Log;
import android.view.inputmethod.InputMethodManager;
import android.widget.Button;
import android.widget.EditText;

import com.jakewharton.rxbinding.view.RxView;
import com.patloew.rxwear.RxWear;
import com.patloew.rxwear.transformers.DataItemGetDataMap;

import hu.akarnokd.rxjava.interop.RxJavaInterop;
import io.reactivex.Observable;
import io.reactivex.disposables.CompositeDisposable;

public class MainActivity extends AppCompatActivity {

    private CoordinatorLayout coordinatorLayout;
    private EditText titleEditText;
    private EditText messageEditText;
    private Button sendButton;
    private EditText persistentEditText;
    private Button setPersistentButton;

    private CompositeDisposable subscription = new CompositeDisposable();
    private Observable<Boolean> validator;

    private RxWear rxWear;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        rxWear = new RxWear(this);

        coordinatorLayout = (CoordinatorLayout) findViewById(R.id.coordinator_layout);
        titleEditText = (EditText) findViewById(R.id.et_title);
        messageEditText = (EditText) findViewById(R.id.et_message);
        sendButton = (Button) findViewById(R.id.bt_send);
        persistentEditText = (EditText) findViewById(R.id.et_persistenttext);
        setPersistentButton = (Button) findViewById(R.id.bt_set_persistent);

        subscription.add(RxJavaInterop.toV2Observable(RxView.clicks(sendButton))
                .doOnNext(click -> hideKeyboard())
                .flatMap(click2 -> validate())
                .filter(isValid -> isValid)
                .flatMap(valid -> rxWear.message().sendDataMapToAllRemoteNodes("/message")
                            .putString("title", titleEditText.getText().toString())
                            .putString("message", messageEditText.getText().toString())
                            .toObservable()
                ).subscribe(requestId -> Snackbar.make(coordinatorLayout, "Sent message", Snackbar.LENGTH_LONG).show(),
                        throwable -> {
                            Log.e("MainActivity", "Error on sending message", throwable);

                        })
        );

        subscription.add(RxJavaInterop.toV2Observable(RxView.clicks(setPersistentButton))
                .doOnNext(click -> hideKeyboard())
                .flatMap(click2 -> rxWear.data().putDataMap().urgent().to("/persistentText").putString("text", persistentEditText.getText().toString()).toObservable())
                .subscribe(dataItem1 -> Snackbar.make(coordinatorLayout, "Set persistent text", Snackbar.LENGTH_LONG).show(),
                        throwable -> {
                            Log.e("MainActivity", "Error on setting persistent text", throwable);

                        }));

        subscription.add(rxWear.data().get("/persistentText")
                .compose(DataItemGetDataMap.noFilter())
                .map(dataMap -> dataMap.getString("text"))
                .subscribe(text -> persistentEditText.setText(text)));
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        subscription.clear();
    }

    private void hideKeyboard() {
        InputMethodManager imm = (InputMethodManager)getSystemService(Context.INPUT_METHOD_SERVICE);
        imm.hideSoftInputFromWindow(sendButton.getWindowToken(), 0);
    }

    private Observable<Boolean> validate() {
        if(validator == null) {
            validator = Observable.fromCallable(() -> {
                boolean valid = true;

                if (TextUtils.isEmpty(titleEditText.getText())) {
                    titleEditText.setError("Please enter title");
                    valid = false;
                }

                if (TextUtils.isEmpty(messageEditText.getText())) {
                    messageEditText.setError("Please enter message");
                    valid = false;
                }

                return valid;
            });
        }

        return validator;
    }
}
