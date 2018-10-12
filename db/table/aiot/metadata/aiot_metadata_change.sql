/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : ytocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2017-04-25 13:26:54
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_metadata_change
-- ----------------------------
DROP TABLE IF EXISTS `aiot_metadata_change`;
CREATE TABLE `aiot_metadata_change` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `collect_time` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `collect_type` tinyint(4) NOT NULL,
  `sensor_type_code` varchar(32) DEFAULT NULL,
  `wpid` int(11) NOT NULL,
  `indicator_code` varchar(32) NOT NULL,
  `mark_code` varchar(32) DEFAULT NULL,
  `newdata` double NOT NULL,
  `olddata` double(20,6) DEFAULT NULL,
  `createtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `creator` varchar(255) DEFAULT NULL,
  `dataversion` varchar(50) DEFAULT NULL COMMENT '数据版本号,依据设备编号,站点编号,操作员编号,策略,时间生成',
  `deviceid` bigint(20) DEFAULT NULL,
  `unitId` int(11) DEFAULT '0',
  `changetype` int(11) NOT NULL DEFAULT '0' COMMENT '0代表新增 1,代表修改',
  `changetime` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '修改/添加时间',
  `userid` int(11) DEFAULT NULL COMMENT '修改/添加人员',
  PRIMARY KEY (`id`),
  KEY `i_collect_time` (`collect_time`) USING BTREE,
  KEY `i_indicator_code` (`indicator_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=91 DEFAULT CHARSET=utf8;
