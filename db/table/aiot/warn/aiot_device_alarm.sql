/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : liaoheocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2018-03-28 09:24:35
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_device_alarm
-- ----------------------------
DROP TABLE IF EXISTS `aiot_device_alarm`;
CREATE TABLE `aiot_device_alarm` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stationid` int(11) NOT NULL,
  `deviceid` int(11) NOT NULL,
  `configid` int(11) NOT NULL,
  `alarmdata` double NOT NULL DEFAULT '0',
  `begintime` timestamp NOT NULL DEFAULT '2000-01-01 01:01:01',
  `endtime` timestamp NOT NULL DEFAULT '2035-01-01 01:01:01',
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_station_device_config` (`stationid`,`deviceid`,`configid`,`begintime`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
