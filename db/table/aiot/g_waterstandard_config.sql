/*
Navicat MySQL Data Transfer

Source Server         : localhost
Source Server Version : 80011
Source Host           : localhost:3306
Source Database       : hnocean

Target Server Type    : MYSQL
Target Server Version : 80011
File Encoding         : 65001

Date: 2018-08-07 09:50:24
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for g_waterstandard_config
-- ----------------------------
DROP TABLE IF EXISTS `g_waterstandard_config`;
CREATE TABLE `g_waterstandard_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `typeid` int(11) NOT NULL DEFAULT '1' COMMENT '0005',
  `classid` int(11) NOT NULL DEFAULT '1' COMMENT '水质等级',
  `classname` varchar(255) NOT NULL COMMENT '水质等级名称',
  `color` varchar(255) DEFAULT '#9dc3e4' COMMENT '提示颜色',
  `ordercode` varchar(5) DEFAULT '999',
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_type_classid` (`typeid`,`classid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of g_waterstandard_config
-- ----------------------------
INSERT INTO `g_waterstandard_config` VALUES ('1', '1', '1', '一类', '#9dc3e4', '001');
INSERT INTO `g_waterstandard_config` VALUES ('2', '1', '2', '二类', '#e2efd9', '002');
INSERT INTO `g_waterstandard_config` VALUES ('3', '1', '3', '三类', '#70ad45', '003');
INSERT INTO `g_waterstandard_config` VALUES ('4', '1', '4', '四类', '#ffd965', '004');
INSERT INTO `g_waterstandard_config` VALUES ('5', '1', '5', '五类', '#ed1e24', '005');
INSERT INTO `g_waterstandard_config` VALUES ('6', '1', '6', '劣五类', '#ad0606', '006');
INSERT INTO `g_waterstandard_config` VALUES ('7', '2', '1', '一类', '#73B2FF', '001');
INSERT INTO `g_waterstandard_config` VALUES ('8', '2', '2', '二类', '#B2DDF7', '002');
INSERT INTO `g_waterstandard_config` VALUES ('9', '2', '3', '三类', '#BEB1A1', '003');
INSERT INTO `g_waterstandard_config` VALUES ('10', '2', '4', '四类', '#9B856E', '004');
INSERT INTO `g_waterstandard_config` VALUES ('11', '2', '5', '劣四类', '#7A624A', '005');
