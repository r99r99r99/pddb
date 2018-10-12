/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.208
Source Server Version : 50625
Source Host           : 192.168.10.208:3306
Source Database       : hnocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2018-02-05 14:47:41
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for aiot_error_data
-- ----------------------------
DROP TABLE IF EXISTS `aiot_error_data`;
CREATE TABLE `aiot_error_data` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stationid` int(11) NOT NULL,
  `indicatorcode` varchar(30) NOT NULL,
  `cal` int(11) NOT NULL DEFAULT '1' COMMENT '计算方法,0019',
  `data` double(255,0) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_error_data` (`stationid`,`indicatorcode`,`data`,`cal`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=84 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of aiot_error_data
-- ----------------------------
INSERT INTO `aiot_error_data` VALUES ('67', '10004', 'NH3Conc', '2', '0');
INSERT INTO `aiot_error_data` VALUES ('68', '10004', 'NH3Conc', '4', '8888');
INSERT INTO `aiot_error_data` VALUES ('61', '10004', 'NO2Conc', '2', '0');
INSERT INTO `aiot_error_data` VALUES ('64', '10004', 'NO2Conc', '4', '8888');
INSERT INTO `aiot_error_data` VALUES ('62', '10004', 'NO3Conc', '2', '0');
INSERT INTO `aiot_error_data` VALUES ('63', '10004', 'NO3Conc', '4', '8888');
INSERT INTO `aiot_error_data` VALUES ('65', '10004', 'PO4Conc', '2', '0');
INSERT INTO `aiot_error_data` VALUES ('66', '10004', 'PO4Conc', '4', '8888');
INSERT INTO `aiot_error_data` VALUES ('69', '10005', 'NH3Conc', '2', '0');
INSERT INTO `aiot_error_data` VALUES ('70', '10005', 'NH3Conc', '4', '8888');
INSERT INTO `aiot_error_data` VALUES ('71', '10005', 'NO2Conc', '2', '0');
INSERT INTO `aiot_error_data` VALUES ('72', '10005', 'NO2Conc', '4', '8888');
INSERT INTO `aiot_error_data` VALUES ('73', '10005', 'NO3Conc', '2', '0');
INSERT INTO `aiot_error_data` VALUES ('74', '10005', 'NO3Conc', '4', '8888');
INSERT INTO `aiot_error_data` VALUES ('75', '10005', 'PO4Conc', '2', '0');
INSERT INTO `aiot_error_data` VALUES ('76', '10005', 'PO4Conc', '4', '8888');
