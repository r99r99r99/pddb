/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.206
Source Server Version : 50625
Source Host           : 192.168.10.206:3306
Source Database       : syocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2017-09-12 09:09:12
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_syndata
-- ----------------------------
DROP TABLE IF EXISTS `aiot_syndata`;
CREATE TABLE `aiot_syndata` (
  `collect_time` datetime NOT NULL,
  `indicator_code` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `data` double DEFAULT NULL,
  `wpid` int(11) DEFAULT NULL,
  `collect_type` int(11) DEFAULT NULL,
  `deviceid` int(11) DEFAULT NULL,
  `unitId` int(11) DEFAULT '0',
  UNIQUE KEY `uniquedata` (`collect_time`,`indicator_code`,`wpid`,`collect_type`,`deviceid`) USING BTREE,
  KEY `i_collect_time` (`collect_time`) USING BTREE,
  KEY `i_indicator_code` (`indicator_code`) USING BTREE,
  KEY `i_wp_id` (`wpid`) USING BTREE,
  KEY `i_deviceid` (`deviceid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of aiot_syndata
-- ----------------------------
