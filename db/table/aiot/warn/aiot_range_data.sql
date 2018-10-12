/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : liaoheocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2018-03-28 09:24:54
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_range_data
-- ----------------------------
DROP TABLE IF EXISTS `aiot_range_data`;
CREATE TABLE `aiot_range_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stationid` int(11) NOT NULL,
  `deviceid` int(11) NOT NULL,
  `indicatorcode` varchar(30) NOT NULL,
  `mindata` double(255,0) NOT NULL DEFAULT '0',
  `maxdata` double(255,0) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_range_data` (`stationid`,`indicatorcode`,`deviceid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=292 DEFAULT CHARSET=utf8;
