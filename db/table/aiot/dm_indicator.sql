/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.80
Source Server Version : 50713
Source Host           : 192.168.10.80:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50713
File Encoding         : 65001

Date: 2018-07-20 10:43:20
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for dm_indicator
-- ----------------------------
DROP TABLE IF EXISTS `dm_indicator`;
CREATE TABLE `dm_indicator` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `code` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `title` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `groupId` bigint(20) DEFAULT NULL,
  `unitId` bigint(20) DEFAULT NULL,
  `description` text CHARACTER SET utf8,
  `isactive` int(3) DEFAULT '1',
  `orderCode` varchar(3) DEFAULT NULL,
  `standardCode` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=631 DEFAULT CHARSET=latin1;
