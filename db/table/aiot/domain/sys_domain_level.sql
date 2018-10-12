/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.80
Source Server Version : 50713
Source Host           : 192.168.10.80:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50713
File Encoding         : 65001

Date: 2018-06-27 15:51:05
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for sys_domain_level
-- ----------------------------
DROP TABLE IF EXISTS `sys_domain_level`;
CREATE TABLE `sys_domain_level` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `domainid` int(11) NOT NULL,
  `code` int(255) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `remark` varchar(255) DEFAULT NULL,
  `color` varchar(255) DEFAULT NULL,
  `ordercode` int(11) DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_level_code,domainid` (`domainid`,`code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8;
