/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : liaoheocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2018-05-25 08:46:59
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_maintain_file
-- ----------------------------
DROP TABLE IF EXISTS `aiot_maintain_file`;
CREATE TABLE `aiot_maintain_file` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mainid` int(11) DEFAULT NULL,
  `realname` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '文件原来的名称',
  `pathname` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '文件路径+保存后的文件名称',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=72 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
