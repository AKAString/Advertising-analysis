package com.qr.spark.useraction.dao.impl

import com.qr.spark.useraction.dao.UserDao
import com.qr.spark.useraction.dao.AdDaoImp
import com.qr.spark.useraction.dao.Session_random_extractDaoImp
import com.qr.spark.useraction.dao.ClickOrderyPayDaoImp
import com.qr.spark.useraction.dao.Top10ClickSessionDaoImp
import com.qr.spark.useraction.dao.Top10ClickDetailDaoImp

object DAOFactory {
  def newUserDao()=new UserDao
  def newAdDao()=new AdDaoImp
  def newSessionRanom_extractDaoImp()=new Session_random_extractDaoImp()
  def newClickOrderyPayDaoImp()=new ClickOrderyPayDaoImp()
  def newTop10ClickSessionDaoImp()=new Top10ClickSessionDaoImp()
  def newTop10ClickDetailDaoImp()=new Top10ClickDetailDaoImp()
}