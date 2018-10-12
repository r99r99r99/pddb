/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2018-05-04 14:44:50
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_metadata_system
-- ----------------------------
DROP TABLE IF EXISTS `aiot_metadata_system`;
CREATE TABLE `aiot_metadata_system` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `collect_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `wpid` int(11) NOT NULL,
  `indicator_code` varchar(32) NOT NULL,
  `data` double(20,6) NOT NULL,
  `createtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `deviceid` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `i_dataversion` (`indicator_code`,`wpid`,`deviceid`,`collect_time`) USING BTREE,
  KEY `i_collect_time` (`collect_time`) USING BTREE,
  KEY `i_indicator_code` (`indicator_code`) USING BTREE,
  KEY `i_wpid` (`wpid`) USING BTREE,
  KEY `i_deviceid` (`deviceid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8;
