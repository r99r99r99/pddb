/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : sdocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2016-07-04 14:08:42
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for device_catalog
-- ----------------------------
DROP TABLE IF EXISTS `device_catalog`;
CREATE TABLE `device_catalog` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `name` varchar(128) CHARACTER SET utf8 DEFAULT NULL COMMENT '设备名称',
  `code` varchar(128) CHARACTER SET utf8 DEFAULT NULL COMMENT '设备编码',
  `deviceModel` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '所属设备型号',
  `brief` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `detail` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `ordercode` varchar(3) CHARACTER SET utf8 DEFAULT '999'
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_code` (`code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=38 DEFAULT CHARSET=latin1;

-- ----------------------------
-- Records of device_catalog
-- ----------------------------
INSERT INTO `device_catalog` VALUES ('17', '水质(Trios)', 'WQTriosShallow', 'WQTriosShallow', 'ManYSI', 'ManYSI', '012', '2', '0');
INSERT INTO `device_catalog` VALUES ('18', '水文数据', 'SontekSL', 'SontekSL', 'AL_ALL_Raw', 'AL_ALL_Raw', '019', '2', '0');
INSERT INTO `device_catalog` VALUES ('19', '降雨量', 'Rain', 'Rain', '介绍', 'AutoLabStandards', '001', '2', '0');
INSERT INTO `device_catalog` VALUES ('20', '水质(EXO)', 'WQEXOShallow', 'WQEXOShallow', 'DeepAL', 'DeepAL', '003', '2', '0');
INSERT INTO `device_catalog` VALUES ('21', '水质(WIZ)营养盐', 'WQWIZMP4', 'WQWIZMP4', '测量深水的数据', '测量深水的数据,简述', '011', '2', '0');
INSERT INTO `device_catalog` VALUES ('22', 'Main', 'Main', 'Main', 'Main', 'Main', '999', '2', '0');
INSERT INTO `device_catalog` VALUES ('23', 'MainAutoLab', 'MainAutoLab', 'MainAutoLab', 'MainAutoLab', 'MainAutoLab', '999', '2', '0');
INSERT INTO `device_catalog` VALUES ('24', 'MainTuner', 'MainTuner', 'MainTuner', 'MainTuner', 'MainTuner', '999', '2', '0');
INSERT INTO `device_catalog` VALUES ('25', 'ShallowAL', 'ShallowAL', 'ShallowAL', 'ShallowAL', 'ShallowAL', '999', '2', '0');
INSERT INTO `device_catalog` VALUES ('26', 'ShallowData', 'ShallowData', 'ShallowData', 'ShallowData', 'ShallowAL', '999', '2', '0');
INSERT INTO `device_catalog` VALUES ('27', 'TimeTable', 'TimeTable', 'TimeTable', 'TimeTable', 'ShallowAL', '999', '2', '0');
INSERT INTO `device_catalog` VALUES ('28', '气象', 'Weather', 'Weather', '气象', '气象', '002', '2', '0');
INSERT INTO `device_catalog` VALUES ('29', 'WIZ总氮', 'WQWIZTN', 'WQWIZTN', '总氮', '用单', '007', '2', '0');
INSERT INTO `device_catalog` VALUES ('30', 'WIZ总磷', 'WQWIZTP', 'WQWIZTP', '总磷', '总磷', '006', '2', '0');
INSERT INTO `device_catalog` VALUES ('31', 'ManYSI', 'ManYSI', 'ManYSI', 'ManYSI', 'ManYSI', '008', '2', '0');
INSERT INTO `device_catalog` VALUES ('32', '系统', 'System', 'System', null, null, '999', '2', '0');
INSERT INTO `device_catalog` VALUES ('33', '油监测', 'Oil', null, null, null, '015', '2', '30');
INSERT INTO `device_catalog` VALUES ('34', 'Ecolab', 'ecolab', null, null, null, '005', '2', '0');
INSERT INTO `device_catalog` VALUES ('35', '斯坦道', 'standao', null, null, null, '014', '2', '1');
INSERT INTO `device_catalog` VALUES ('36', 'E+H(氨氮)', 'eh', null, null, null, '013', '2', '0');
INSERT INTO `device_catalog` VALUES ('37', 'E+H(常规)', 'ehc', null, null, null, '004', '2', '0');
