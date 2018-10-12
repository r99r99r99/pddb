/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : liaoheocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2018-05-25 08:47:13
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_maintain_device_indicator
-- ----------------------------
DROP TABLE IF EXISTS `aiot_maintain_device_indicator`;
CREATE TABLE `aiot_maintain_device_indicator` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mainid` int(11) NOT NULL,
  `deviceid` int(11) NOT NULL,
  `indicatorcode` varchar(30) COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_main_device_indicator` (`mainid`,`deviceid`,`indicatorcode`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=2382 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
