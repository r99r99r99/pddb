/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : liaoheocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2018-05-25 08:46:51
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_maintain
-- ----------------------------
DROP TABLE IF EXISTS `aiot_maintain`;
CREATE TABLE `aiot_maintain` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stationid` int(11) NOT NULL,
  `beginTime` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `endtime` timestamp NULL DEFAULT NULL,
  `userid` int(11) NOT NULL,
  `status` int(11) NOT NULL DEFAULT '1' COMMENT '1,维护中添加开始时间 2，维护结束添加结束时间  3，已打印不可更改',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=132 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
