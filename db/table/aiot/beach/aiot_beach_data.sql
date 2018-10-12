/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.80
Source Server Version : 50713
Source Host           : 192.168.10.80:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50713
File Encoding         : 65001

Date: 2018-06-27 15:48:19
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_beach_data
-- ----------------------------
DROP TABLE IF EXISTS `aiot_beach_data`;
CREATE TABLE `aiot_beach_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stationid` int(11) DEFAULT NULL,
  `collecttime` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `createtime` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `userid` int(11) DEFAULT NULL,
  `code` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `data` double NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_stationid_collect_code` (`stationid`,`collecttime`,`code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=471 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
