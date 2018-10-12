/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.80
Source Server Version : 50713
Source Host           : 192.168.10.80:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50713
File Encoding         : 65001

Date: 2018-06-27 15:51:30
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for sys_domain_threshold
-- ----------------------------
DROP TABLE IF EXISTS `sys_domain_threshold`;
CREATE TABLE `sys_domain_threshold` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `levelid` int(11) NOT NULL,
  `mincal` int(11) NOT NULL DEFAULT '1' COMMENT '1 <= 2 <',
  `min` double NOT NULL DEFAULT '0',
  `maxcal` int(11) NOT NULL DEFAULT '1' COMMENT '1>=  2>',
  `max` double NOT NULL DEFAULT '0',
  `indicatorcode` varchar(50) DEFAULT NULL COMMENT '参数',
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_level_indicator` (`levelid`,`indicatorcode`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=61 DEFAULT CHARSET=utf8;
