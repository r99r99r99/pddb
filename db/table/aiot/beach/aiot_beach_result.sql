/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.80
Source Server Version : 50713
Source Host           : 192.168.10.80:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50713
File Encoding         : 65001

Date: 2018-06-27 15:48:42
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_beach_result
-- ----------------------------
DROP TABLE IF EXISTS `aiot_beach_result`;
CREATE TABLE `aiot_beach_result` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stationid` int(11) DEFAULT NULL,
  `stattype` int(11) DEFAULT NULL,
  `xtime` varchar(30) COLLATE utf8_bin DEFAULT NULL,
  `ydata` int(11) DEFAULT NULL,
  `remark` varchar(500) COLLATE utf8_bin DEFAULT NULL,
  `type` int(11) DEFAULT NULL COMMENT '0游泳指数1水文要素2气象要素3水质状况4健康指数5沙滩环境6水母',
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_stationi_stattype_xtime` (`stationid`,`stattype`,`xtime`,`type`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=191 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
