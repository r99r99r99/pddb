/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.211
Source Server Version : 50637
Source Host           : 192.168.10.211:3306
Source Database       : bhocean

Target Server Type    : MYSQL
Target Server Version : 50637
File Encoding         : 65001

Date: 2017-12-04 11:29:34
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for g_command
-- ----------------------------
DROP TABLE IF EXISTS `g_command`;
CREATE TABLE `g_command` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(255) COLLATE utf8_bin DEFAULT NULL,
  `type` varchar(255) COLLATE utf8_bin DEFAULT '1' COMMENT '1,接口对象类别,2,对象属性类别,3,命令类别',
  `name` varchar(255) COLLATE utf8_bin DEFAULT NULL,
  `ename` varchar(255) COLLATE utf8_bin DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `un_command` (`code`,`type`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=222 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of g_command
-- ----------------------------
INSERT INTO `g_command` VALUES ('1', '01', '1', '站点', 'Station');
INSERT INTO `g_command` VALUES ('11', '02', '1', '监测仪', 'Instrument');
INSERT INTO `g_command` VALUES ('21', '03', '1', '辅助设备', 'Device');
INSERT INTO `g_command` VALUES ('31', '01', '2', '信息', 'Information');
INSERT INTO `g_command` VALUES ('41', '02', '2', '状态', 'Status');
INSERT INTO `g_command` VALUES ('51', '03', '2', '监测数据', 'Monitoringdata');
INSERT INTO `g_command` VALUES ('61', '04', '2', '系统开关', 'Switch');
INSERT INTO `g_command` VALUES ('71', '05', '2', '系统信息同步', 'Synchronization');
INSERT INTO `g_command` VALUES ('81', '06', '2', '系统授时', 'TimeService');
INSERT INTO `g_command` VALUES ('91', '07', '2', '限温保护', 'TemperatureProtect');
INSERT INTO `g_command` VALUES ('101', '08', '2', '数据存储间隔', 'Storageinterval');
INSERT INTO `g_command` VALUES ('111', '09', '2', '数据上报间隔', 'DataReportinginterval');
INSERT INTO `g_command` VALUES ('121', '10', '2', '警报上下限', 'Limit');
INSERT INTO `g_command` VALUES ('131', '11', '2', '监测仪校准', 'InstrumentCalibration');
INSERT INTO `g_command` VALUES ('141', '12', '2', '设备清洗', 'EquipmentClean');
INSERT INTO `g_command` VALUES ('151', '13', '2', '状态上报间隔', 'StatusReportinginterval');
INSERT INTO `g_command` VALUES ('161', '14', '2', '心跳检测', 'HeartbeatDetection');
INSERT INTO `g_command` VALUES ('171', '15', '2', '警报', 'Alarm');
INSERT INTO `g_command` VALUES ('181', '16', '2', '补帧', 'FrameRepairing');
INSERT INTO `g_command` VALUES ('191', '17', '2', '运行模式', 'RunningMode');
INSERT INTO `g_command` VALUES ('201', '01', '3', '控制接口', 'Control');
INSERT INTO `g_command` VALUES ('211', '02', '3', '查询接口', 'Query');
INSERT INTO `g_command` VALUES ('221', '03', '3', '上报接口', 'Report');
