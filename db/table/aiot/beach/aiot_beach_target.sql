/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.80
Source Server Version : 50713
Source Host           : 192.168.10.80:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50713
File Encoding         : 65001

Date: 2018-06-27 15:48:59
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_beach_target
-- ----------------------------
DROP TABLE IF EXISTS `aiot_beach_target`;
CREATE TABLE `aiot_beach_target` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `stationid` int(11) DEFAULT NULL,
  `deviceid` int(11) DEFAULT NULL,
  `indicatorcode` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `tounit` double NOT NULL DEFAULT '1' COMMENT '从自动读取到功能区转换倍数',
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_st_in` (`code`,`stationid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
