/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2018-05-14 10:11:01
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_metadata_manual
-- ----------------------------
DROP TABLE IF EXISTS `aiot_metadata_manual`;
CREATE TABLE `aiot_metadata_manual` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stationid` int(11) NOT NULL,
  `indicatorcode` varchar(30) NOT NULL,
  `data` double DEFAULT NULL,
  `collecttime` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `createtime` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `userid` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_station_indicator_collect` (`stationid`,`indicatorcode`,`collecttime`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=112 DEFAULT CHARSET=utf8;
