/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : liaohe_oceantech

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2015-12-24 10:48:23
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for resources
-- ----------------------------
DROP TABLE IF EXISTS `resources`;
CREATE TABLE `resources` (
  `ID` varchar(32) NOT NULL,
  `NAME` varchar(255) DEFAULT NULL COMMENT '��Դ����',
  `URL` varchar(255) NOT NULL COMMENT '��Դ��Ӧurl',
  `REMARK` varchar(2000) DEFAULT NULL COMMENT '��ע',
  `AUTH_ID` varchar(32) NOT NULL COMMENT 'Ȩ��id',
  PRIMARY KEY (`ID`),
  KEY `RESOURCES_F_AUTH_ID` (`AUTH_ID`) USING BTREE,
  CONSTRAINT `resources_ibfk_1` FOREIGN KEY (`AUTH_ID`) REFERENCES `authority` (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='��Դ��';

-- ----------------------------
-- Records of resources
-- ----------------------------
