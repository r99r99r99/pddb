/*
Navicat MySQL Data Transfer

Source Server         : 192.168.10.80
Source Server Version : 50713
Source Host           : 192.168.10.80:3306
Source Database       : ytsocean

Target Server Type    : MYSQL
Target Server Version : 50713
File Encoding         : 65001

Date: 2018-07-20 10:43:29
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for g_unit
-- ----------------------------
DROP TABLE IF EXISTS `g_unit`;
CREATE TABLE `g_unit` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `code` varchar(20) NOT NULL,
  `groupId` bigint(20) DEFAULT NULL,
  `name` varchar(32) NOT NULL,
  `description` varchar(255) DEFAULT NULL COMMENT '简写',
  `logo` varchar(255) DEFAULT NULL,
  `standardcode` varchar(30) DEFAULT NULL COMMENT '标准编码',
  PRIMARY KEY (`id`),
  UNIQUE KEY `code` (`code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=202 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of g_unit
-- ----------------------------
INSERT INTO `g_unit` VALUES ('11', 'meter', '1', '米', null, 'm', '0101');
INSERT INTO `g_unit` VALUES ('12', 'kilometer', '1', '千米', null, 'km', '0102');
INSERT INTO `g_unit` VALUES ('13', 'centimeter', '1', '厘米', null, 'cm', '0103');
INSERT INTO `g_unit` VALUES ('14', 'millimeter', '1', '毫米', null, 'mm', '0104');
INSERT INTO `g_unit` VALUES ('15', 'micron', '1', '微米', null, 'μm', '0105');
INSERT INTO `g_unit` VALUES ('16', 'nanometer', '1', '纳米', null, 'nm', '0106');
INSERT INTO `g_unit` VALUES ('17', 'nauticalmile', '1', '海里', '1海里=1852米=1.852千米', 'nmile', '0107');
INSERT INTO `g_unit` VALUES ('21', 'Squaremeter', '2', '平方米', null, 'm²', '0201');
INSERT INTO `g_unit` VALUES ('22', 'Squarekilometer', '2', '平方公里', null, 'km²', '0202');
INSERT INTO `g_unit` VALUES ('23', 'Squarecentimetre', '2', '平方厘米', null, 'cm²', '0203');
INSERT INTO `g_unit` VALUES ('24', 'Squaremillimeter', '2', '平方毫米', null, 'mm²', '0204');
INSERT INTO `g_unit` VALUES ('25', 'Hectare', '2', '公顷', '1公顷=10000米²=10∧4 米²', 'hm²', '0205');
INSERT INTO `g_unit` VALUES ('31', 'hour', '3', '小时', null, 'h', '0301');
INSERT INTO `g_unit` VALUES ('32', 'min', '3', '分钟', null, 'min', '0302');
INSERT INTO `g_unit` VALUES ('33', 'second', '3', '秒', null, 's', '0303');
INSERT INTO `g_unit` VALUES ('34', 'millisecond', '3', '毫秒', null, 'ms', '0304');
INSERT INTO `g_unit` VALUES ('35', 'microsecond', '3', '微秒', null, 'μs', '0305');
INSERT INTO `g_unit` VALUES ('36', 'nanosecond', '3', '纳秒', null, 'ns', '0306');
INSERT INTO `g_unit` VALUES ('37', 'picosecond ', '3', '皮秒', '1h=60min;1min=60s;1s=10^3ms=10^6μs=10^9ns=10^12ps', 'ps', '0307');
INSERT INTO `g_unit` VALUES ('41', 'Stere', '4', '立方米', null, 'm³', '0401');
INSERT INTO `g_unit` VALUES ('42', 'Cubickilometer', '4', '立方千米', null, 'km³', '0402');
INSERT INTO `g_unit` VALUES ('43', 'Cubiccentimeter', '4', '立方厘米', null, 'cm³', '0403');
INSERT INTO `g_unit` VALUES ('44', 'Cubicmillimeter', '4', '立方毫米', null, 'mm³', '0404');
INSERT INTO `g_unit` VALUES ('51', 'litre', '5', '升', null, 'L', '0501');
INSERT INTO `g_unit` VALUES ('52', 'milliliter', '5', '毫升', null, 'mL', '0502');
INSERT INTO `g_unit` VALUES ('61', 'kilogram', '6', '千克', null, 'kg', '0601');
INSERT INTO `g_unit` VALUES ('62', 'gram', '6', '克', null, 'g', '0602');
INSERT INTO `g_unit` VALUES ('63', 'milligram', '6', '毫克', null, 'mg', '0603');
INSERT INTO `g_unit` VALUES ('64', 'ton', '6', '吨', null, 't', '0604');
INSERT INTO `g_unit` VALUES ('65', 'Atomicmassunit', '6', '原子质量单位', null, 'u', '0605');
INSERT INTO `g_unit` VALUES ('66', 'microgram', '6', '微克', null, 'μg', '0606');
INSERT INTO `g_unit` VALUES ('71', 'newton', '7', '牛顿', null, 'N', '0701');
INSERT INTO `g_unit` VALUES ('81', 'pascal', '8', '帕斯卡', null, 'Pa', '0801');
INSERT INTO `g_unit` VALUES ('82', 'kilopascal', '8', '千帕斯卡', '1千帕=1000牛/米²', 'kPa', '0802');
INSERT INTO `g_unit` VALUES ('83', 'hPa', '8', '百帕', null, 'hPa', '0803');
INSERT INTO `g_unit` VALUES ('91', 'Kelvin', '9', '开尔文', null, 'K', '0901');
INSERT INTO `g_unit` VALUES ('92', 'Degrees', '9', '摄氏度', null, '℃', '0902');
INSERT INTO `g_unit` VALUES ('101', 'joule', '10', '焦耳', '1焦=1牛·米', 'J', '1001');
INSERT INTO `g_unit` VALUES ('102', 'Electron-volts', '10', '电子伏', '1电子伏=1.602189×10∧-19 焦', 'eV', '1002');
INSERT INTO `g_unit` VALUES ('103', '1000wattshour', '10', '千瓦特小时', '1千瓦·时=3.6×10∧6 焦', 'kW·h', '1003');
INSERT INTO `g_unit` VALUES ('104', 'cal', '10', '卡', '1焦耳=4.2卡', 'cal', '1004');
INSERT INTO `g_unit` VALUES ('105', 'kj/kg', '10', '焓/千克', null, 'kj/kg', '0105');
INSERT INTO `g_unit` VALUES ('111', 'watt', '11', '瓦特', null, 'W', '1101');
INSERT INTO `g_unit` VALUES ('112', 'kilowatt', '11', '千瓦特', '1千瓦=1000焦/秒', 'kW', '1102');
INSERT INTO `g_unit` VALUES ('121', 'radian', '12', '弧度', '1弧度=180/π 度', 'rad', '1201');
INSERT INTO `g_unit` VALUES ('122', 'degree', '12', '度', '1度=（π∕180）弧度=60分=3600秒', '°', '1202');
INSERT INTO `g_unit` VALUES ('123', 'dmin', '12', '分', '1分=（π/10800）弧度=60秒', '′', '1203');
INSERT INTO `g_unit` VALUES ('124', 'dsec', '12', '秒', '1秒=（π/64800）弧度', '″', '1204');
INSERT INTO `g_unit` VALUES ('131', 'Hz', '13', '赫兹', null, 'Hz', '1301');
INSERT INTO `g_unit` VALUES ('141', 'm/s', '14', '米每秒', null, 'm/s', '1401');
INSERT INTO `g_unit` VALUES ('142', 'km/h', '14', '千米每小时', null, 'km/h', '1402');
INSERT INTO `g_unit` VALUES ('143', 'kn', '14', '节', null, 'kn', '1403');
INSERT INTO `g_unit` VALUES ('144', 'cms', '14', '厘米/秒', null, 'cm/s', '1404');
INSERT INTO `g_unit` VALUES ('145', 'm3/s', '14', '立方米/秒', null, 'm3/s', '1405');
INSERT INTO `g_unit` VALUES ('146', 'mm/h', '14', '毫米/小时', null, 'mm/h', '1406');
INSERT INTO `g_unit` VALUES ('151', 'mg/L', '15', '毫克/升', null, 'mg/L', '1501');
INSERT INTO `g_unit` VALUES ('152', 'ug/L', '15', '微克/升', null, 'μg/L', '1502');
INSERT INTO `g_unit` VALUES ('153', 'ng/L', '15', '纳克/升', null, 'ng/L', '1503');
INSERT INTO `g_unit` VALUES ('154', 'NTU', '15', '浊度单位', null, 'NTU', '1504');
INSERT INTO `g_unit` VALUES ('155', 'percentage', '15', '百分百', null, '%', '1505');
INSERT INTO `g_unit` VALUES ('156', 'ge/L', '15', '个/升', null, 'ge/L', '1506');
INSERT INTO `g_unit` VALUES ('157', 'mg/m3', '15', '毫克/立方米', null, 'mg/m3', '1507');
INSERT INTO `g_unit` VALUES ('161', 'μscm', '16', '微西门子/厘米', null, 'μs/cm', '1601');
INSERT INTO `g_unit` VALUES ('162', 'ms/cm', '16', '毫西门子每厘米', null, 'ms/cm', '1602');
INSERT INTO `g_unit` VALUES ('163', 'volt', '16', '伏特', null, 'V', '1603');
INSERT INTO `g_unit` VALUES ('164', 'ampere', '16', '安培', null, 'A', '1604');
INSERT INTO `g_unit` VALUES ('165', 'Ω', '16', '欧姆', null, 'Ω', '1605');
INSERT INTO `g_unit` VALUES ('166', 'μS/m', '16', '微西门子/米', null, 'μS/m', '1606');
INSERT INTO `g_unit` VALUES ('171', 'ge', '17', '个', null, '个', '1701');
INSERT INTO `g_unit` VALUES ('181', 'Lux', '18', '勒克斯', null, 'Lux', '1801');
INSERT INTO `g_unit` VALUES ('191', 'becquerel', '19', '贝克勒尔', null, 'BQ', '1901');
INSERT INTO `g_unit` VALUES ('192', 'becquerel/L', '19', '贝克勒尔/升', null, 'BQ/L', '1902');
INSERT INTO `g_unit` VALUES ('193', 'μSv/h', '19', '微希沃特/小时', null, 'μSv/h', '1903');
INSERT INTO `g_unit` VALUES ('201', 'empty', '20', '空单位', null, '', '2001');
