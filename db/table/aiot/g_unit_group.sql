/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.80
Source Server Version : 50713
Source Host           : 192.168.10.80:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50713
File Encoding         : 65001

Date: 2018-07-20 10:43:36
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for g_unit_group
-- ----------------------------
DROP TABLE IF EXISTS `g_unit_group`;
CREATE TABLE `g_unit_group` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `name` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `description` varchar(255) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=22 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of g_unit_group
-- ----------------------------
INSERT INTO `g_unit_group` VALUES ('1', 'length', '长度', '长度单位');
INSERT INTO `g_unit_group` VALUES ('2', 'area', '面积', '面积单位');
INSERT INTO `g_unit_group` VALUES ('3', 'time', '时间', '时间单位');
INSERT INTO `g_unit_group` VALUES ('4', 'volume', '体积', '体积单位');
INSERT INTO `g_unit_group` VALUES ('5', 'cubage', '容积', '容积单位');
INSERT INTO `g_unit_group` VALUES ('6', 'mass', '质量', '质量单位');
INSERT INTO `g_unit_group` VALUES ('7', 'force', '力量', '力量单位');
INSERT INTO `g_unit_group` VALUES ('8', 'pressure', '压力', '压力单位');
INSERT INTO `g_unit_group` VALUES ('9', 'temperature', '温度', '温度单位');
INSERT INTO `g_unit_group` VALUES ('10', 'energy', '能', '能单位');
INSERT INTO `g_unit_group` VALUES ('11', 'power', '功率', '功率单位');
INSERT INTO `g_unit_group` VALUES ('12', 'angle', '角度', '角度单位');
INSERT INTO `g_unit_group` VALUES ('13', 'rate', '频率', '频率单位');
INSERT INTO `g_unit_group` VALUES ('14', 'speed', '速度', '速度单位');
INSERT INTO `g_unit_group` VALUES ('15', 'content', '含量', '含量单位');
INSERT INTO `g_unit_group` VALUES ('16', 'electric', '电', '电学单位');
INSERT INTO `g_unit_group` VALUES ('17', 'num', '数量', '数量单位');
INSERT INTO `g_unit_group` VALUES ('18', 'light', '光照', '光照单位');
INSERT INTO `g_unit_group` VALUES ('19', 'radioactivity', '放射性', '放射性单位');
INSERT INTO `g_unit_group` VALUES ('20', 'else', '其他', '其他单位');
