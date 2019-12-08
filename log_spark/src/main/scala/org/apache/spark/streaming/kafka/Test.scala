package org.apache.spark.streaming.kafka

import com.alibaba.fastjson.TypeReference

/**
  * Created by Axin in 2019/11/24 16:37  
  */
object Test {

  def main(args: Array[String]): Unit = {
     //val json:String = '{"rksj":"1573830566","latitude":"24.000000","imsi":"000000000000000","accept_message":"","phone_mac":"aa-aa-aa-aa-aa-aa","device_mac":"bb-bb-bb-bb-bb-bb","message_time":"1789098762","filename":"wechat_source1_1111169.txt","phone":"18609765432","absolute_filename":"//usr//chl//data//successful//2019-11-15/data/filedir/wechat_source1_1111169.txt","device_number":"32109231","imei":"000000000000000","collect_time":"1557305988","id":"87e110fc387142f3bc79466638c4c97e","send_message":"","object_username":"judy","table":"wechat","longitude":"23.000000","username":"andiy"}'
    val json:String = "{'rksj':'1573830566','latitude':'24.000000','imsi':'000000000000000','accept_message':'','phone_mac':'aa-aa-aa-aa-aa-aa','device_mac':'bb-bb-bb-bb-bb-bb','message_time':'1789098762','filename':'wechat_source1_1111169.txt','phone':'18609765432','absolute_filename':'/usr/chl/data/successful//2019-11-15/data/filedir/wechat_source1_1111169.txt','device_number':'32109231','imei':'000000000000000','collect_time':'1557305988','id':'87e110fc387142f3bc79466638c4c97e','send_message':'','object_username':'judy','table':'wechat','longitude':'23.000000','username':'andiy'}"

    println(json)
    var res : java.util.Map[String,String] = null
    try {
      res = com.alibaba.fastjson.JSON.parseObject(json, new TypeReference[java.util.Map[String, String]]() {})
    } catch {
      case e: Exception => println("失败")
    }
    println(res)

  }

}
