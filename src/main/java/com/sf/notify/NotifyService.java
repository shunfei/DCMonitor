package com.sf.notify;

import retrofit.http.Body;
import retrofit.http.POST;
import retrofit.http.Query;

interface NotifyService {
  @POST("/email/send")
  public Notify.Result send(@Body Notify.Email mail, @Query("sync") int sync);

  @POST("/sms/send")
  public Notify.Result send(@Body Notify.SMS sms, @Query("sync") int sync);

  @POST("/qywechat/send")
  public Notify.Result send(@Body Notify.Wechat wechat, @Query("sync") int sync);

}
