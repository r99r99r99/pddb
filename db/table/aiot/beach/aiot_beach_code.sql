/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.80
Source Server Version : 50713
Source Host           : 192.168.10.80:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50713
File Encoding         : 65001

Date: 2018-06-27 15:46:37
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_beach_code
-- ----------------------------
DROP TABLE IF EXISTS `aiot_beach_code`;
CREATE TABLE `aiot_beach_code` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `groupid` int(11) DEFAULT NULL,
  `code` varchar(30) COLLATE utf8_bin NOT NULL,
  `name` varchar(255) COLLATE utf8_bin NOT NULL,
  `type` int(11) DEFAULT '1' COMMENT '1代表数字类型 2代表配置类型',
  `unit` varchar(255) COLLATE utf8_bin DEFAULT NULL,
  `ordercode` varchar(3) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=181 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
