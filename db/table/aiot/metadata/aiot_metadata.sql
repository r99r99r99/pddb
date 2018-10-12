/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.206
Source Server Version : 50625
Source Host           : 192.168.10.206:3306
Source Database       : syocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2017-09-12 09:09:05
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_metadata
-- ----------------------------
DROP TABLE IF EXISTS `aiot_metadata`;
CREATE TABLE `aiot_metadata` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `collect_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `collect_type` tinyint(4) NOT NULL,
  `sensor_type_code` varchar(32) DEFAULT NULL,
  `wpid` int(11) NOT NULL,
  `indicator_code` varchar(32) NOT NULL,
  `mark_code` varchar(32) DEFAULT NULL,
  `data` double(20,6) NOT NULL,
  `createtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `creator` varchar(255) DEFAULT NULL,
  `dataversion` varchar(50) DEFAULT NULL COMMENT '数据版本号,依据设备编号,站点编号,操作员编号,策略,时间生成',
  `deviceid` bigint(20) DEFAULT NULL,
  `unitId` int(11) DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `i_dataversion` (`indicator_code`,`wpid`,`deviceid`,`collect_time`,`collect_type`) USING BTREE,
  KEY `i_collect_time` (`collect_time`) USING BTREE,
  KEY `i_indicator_code` (`indicator_code`) USING BTREE,
  KEY `i_wp_id` (`wpid`) USING BTREE,
  KEY `i_deviceid` (`deviceid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of aiot_metadata
-- ----------------------------
