/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : sdocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2016-06-23 11:36:42
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
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=88 DEFAULT CHARSET=latin1;

-- ----------------------------
-- Records of dm_indicator
-- ----------------------------
INSERT INTO `dm_indicator` VALUES ('3', 'Auto_Abs210', 'Auto_Abs210', '1', '1', '备注', '1', '001');
INSERT INTO `dm_indicator` VALUES ('4', 'Auto_Abs254', 'Auto_Abs254', '1', '1', null, '1', '002');
INSERT INTO `dm_indicator` VALUES ('5', 'Auto_Abs360', 'Auto_Abs360', '1', '1', null, '1', '003');
INSERT INTO `dm_indicator` VALUES ('6', 'Auto_CODeq_Raw', 'Auto_CODeq_Raw', '1', '1', null, '1', '004');
INSERT INTO `dm_indicator` VALUES ('7', 'Auto_CODeq_Scaled', '化学需氧量', '1', '14', null, '1', '005');
INSERT INTO `dm_indicator` VALUES ('8', 'Auto_NO2_Concentration_Raw', 'Auto_NO2_Concentration_Raw', '1', '1', null, '1', '006');
INSERT INTO `dm_indicator` VALUES ('9', 'Auto_NO2_Concentration_Scaled', '亚硝酸盐(Trios)', '1', '14', null, '1', '022');
INSERT INTO `dm_indicator` VALUES ('10', 'Auto_NO3_Concentration_Raw', '硝酸盐(Trios)', '1', '1', '', '1', '008');
INSERT INTO `dm_indicator` VALUES ('11', 'Auto_NO3_Concentration_Scaled', '硝酸盐(Trios)', '1', '21', null, '1', '021');
INSERT INTO `dm_indicator` VALUES ('12', 'Auto_SAC254', 'Auto_SAC254', '1', '1', null, '1', '010');
INSERT INTO `dm_indicator` VALUES ('13', 'Auto_TSSeq_Raw', 'Auto_TSSeq_Raw', '1', '1', null, '1', '011');
INSERT INTO `dm_indicator` VALUES ('14', 'Auto_TSSeq_Scaled', 'Auto_TSSeq_Scaled', '1', '1', null, '1', '012');
INSERT INTO `dm_indicator` VALUES ('15', 'BGA', '蓝绿藻', '1', '21', null, '1', '013');
INSERT INTO `dm_indicator` VALUES ('16', 'Chlorophyll', '叶绿素', '1', '21', null, '1', '014');
INSERT INTO `dm_indicator` VALUES ('17', 'Conductivity', '电导率', '1', '17', null, '1', '015');
INSERT INTO `dm_indicator` VALUES ('18', 'Cross_Section_Area', 'Cross_Section_Area', '1', '1', null, '1', '016');
INSERT INTO `dm_indicator` VALUES ('19', 'DO', '溶解氧', '1', '14', null, '1', '017');
INSERT INTO `dm_indicator` VALUES ('20', 'Flow', '流量', '1', '26', null, '1', '018');
INSERT INTO `dm_indicator` VALUES ('21', 'Nitrate', 'Nitrate', '1', '1', null, '1', '019');
INSERT INTO `dm_indicator` VALUES ('22', 'pH', 'pH', '1', '4', null, '1', '020');
INSERT INTO `dm_indicator` VALUES ('23', 'River_Average_Velocity', '平均流速', '1', '37', null, '1', '021');
INSERT INTO `dm_indicator` VALUES ('24', 'Salinity', '盐度', '1', '27', null, '1', '022');
INSERT INTO `dm_indicator` VALUES ('25', 'Shallow_CycleType', 'Shallow_CycleType', '1', '1', null, '1', '023');
INSERT INTO `dm_indicator` VALUES ('26', 'Shallow_NH3FinalOD', 'Shallow_NH3FinalOD', '1', '1', null, '1', '024');
INSERT INTO `dm_indicator` VALUES ('27', 'Shallow_NH3StartOD', 'Shallow_NH3StartOD', '1', '1', null, '1', '025');
INSERT INTO `dm_indicator` VALUES ('28', 'Shallow_NH3_Conc', '氨氮', '1', '21', null, '1', '034');
INSERT INTO `dm_indicator` VALUES ('29', 'Shallow_NO2FinalOD', 'Shallow_NO2FinalOD', '1', '1', null, '1', '027');
INSERT INTO `dm_indicator` VALUES ('30', 'Shallow_NO2StartOD', 'Shallow_NO2StartOD', '1', '1', null, '1', '028');
INSERT INTO `dm_indicator` VALUES ('31', 'Shallow_NO2_Conc', '亚硝酸盐(wiz)', '1', '21', null, '1', '033');
INSERT INTO `dm_indicator` VALUES ('32', 'Shallow_NO3FinalOD', 'Shallow_NO3FinalOD', '1', '1', null, '1', '030');
INSERT INTO `dm_indicator` VALUES ('33', 'Shallow_NO3StartOD', 'Shallow_NO3StartOD', '1', '1', null, '1', '031');
INSERT INTO `dm_indicator` VALUES ('34', 'Shallow_NO3_Conc', '硝酸盐(wiz)', '1', '21', null, '1', '032');
INSERT INTO `dm_indicator` VALUES ('35', 'Shallow_PO4FinalOD', 'Shallow_PO4FinalOD', '1', '1', null, '1', '033');
INSERT INTO `dm_indicator` VALUES ('36', 'Shallow_PO4StartOD', 'Shallow_PO4StartOD', '1', '1', null, '1', '034');
INSERT INTO `dm_indicator` VALUES ('37', 'Shallow_PO4_Conc', '磷酸盐', '1', '21', null, '1', '035');
INSERT INTO `dm_indicator` VALUES ('38', 'Sontek_CellEnd', 'Sontek_CellEnd', '1', '1', null, '1', '036');
INSERT INTO `dm_indicator` VALUES ('39', 'Sontek_Flow', 'Sontek_Flow', '1', '1', null, '1', '037');
INSERT INTO `dm_indicator` VALUES ('40', 'Sontek_Pressure', 'Sontek_Pressure', '1', '1', null, '1', '038');
INSERT INTO `dm_indicator` VALUES ('41', 'Sontek_SNR', 'Sontek_SNR', '1', '1', null, '1', '039');
INSERT INTO `dm_indicator` VALUES ('42', 'Sontek_Speed', '流速', '1', '22', null, '1', '040');
INSERT INTO `dm_indicator` VALUES ('43', 'Sontek_Stage', 'Sontek_Stage', '1', '1', null, '1', '041');
INSERT INTO `dm_indicator` VALUES ('44', 'Sontek_Temp', '水温', '1', '2', null, '1', '042');
INSERT INTO `dm_indicator` VALUES ('45', 'Sontek_XVel', 'Sontek_XVel', '1', '1', null, '1', '043');
INSERT INTO `dm_indicator` VALUES ('46', 'Sontek_YVel', 'Sontek_YVel', '1', '1', null, '1', '044');
INSERT INTO `dm_indicator` VALUES ('47', 'Temperature', 'Temperature', '1', '1', null, '1', '045');
INSERT INTO `dm_indicator` VALUES ('48', 'Turbidity', '浊度', '1', '16', null, '1', '046');
INSERT INTO `dm_indicator` VALUES ('49', 'Water_Level_Above_River_Bed', '水位', '1', '15', null, '1', '047');
INSERT INTO `dm_indicator` VALUES ('50', 'Water_Level_Gauge', 'Water_Level_Gauge', '1', '1', null, '1', '048');
INSERT INTO `dm_indicator` VALUES ('51', 'WiperPos', 'WiperPos', '1', '1', null, '1', '049');
INSERT INTO `dm_indicator` VALUES ('52', 'TBRG_RainTotal', '降雨量', '1', '8', null, '1', '050');
INSERT INTO `dm_indicator` VALUES ('53', 'WS600_AirTemperatureAct', '空气温度', '1', '2', null, '1', '051');
INSERT INTO `dm_indicator` VALUES ('54', 'WS600_RelHumidityAct', '相对湿度', '1', '3', null, '1', '052');
INSERT INTO `dm_indicator` VALUES ('55', 'WS600_RelAirPressure', '相对气压', '1', '23', null, '1', '053');
INSERT INTO `dm_indicator` VALUES ('56', 'WS600_WindSpeedAvg', '平均风速', '1', '22', null, '1', '054');
INSERT INTO `dm_indicator` VALUES ('57', 'WS600_WindSpeedMax', '最大风速', '1', '22', null, '1', '055');
INSERT INTO `dm_indicator` VALUES ('58', 'WS600_WindSpeedMin', '最小风速', '1', '22', null, '1', '056');
INSERT INTO `dm_indicator` VALUES ('59', 'WS600_WindDirectionAct', '风向', '1', '5', null, '1', '057');
INSERT INTO `dm_indicator` VALUES ('60', 'Shallow_TN', '总氮', '1', '21', null, '1', '058');
INSERT INTO `dm_indicator` VALUES ('61', 'Shallow_TP', '总磷', '1', '21', null, '1', '059');
INSERT INTO `dm_indicator` VALUES ('62', 'ManualBGA_YSI', 'ManualBGA_YSI', '1', '1', null, '1', '999');
INSERT INTO `dm_indicator` VALUES ('63', 'ManualChlor_YSI', 'ManualChlor_YSI', '1', '1', null, '1', '999');
INSERT INTO `dm_indicator` VALUES ('64', 'ManualCond_YSI', 'ManualCond_YSI', '1', '1', null, '1', '999');
INSERT INTO `dm_indicator` VALUES ('65', 'ManualDO_YSI', 'ManualDO_YSI', '1', '1', null, '1', '999');
INSERT INTO `dm_indicator` VALUES ('66', 'ManualTemp_YSI', 'ManualTemp_YSI', '1', '1', null, '1', '999');
INSERT INTO `dm_indicator` VALUES ('67', 'ManualTurb_YSI', 'ManualTurb_YSI', '1', '1', null, '1', '999');
INSERT INTO `dm_indicator` VALUES ('68', 'ManualpH_YSI', 'ManualpH_YSI', '1', '1', null, '1', '999');
INSERT INTO `dm_indicator` VALUES ('69', 'Supply_Voltage', '供电电压', '3', '25', '供电电压', '1', '001');
INSERT INTO `dm_indicator` VALUES ('70', 'Panel_Temperature', '面板温度', '3', '2', '面板温度', '1', '002');
INSERT INTO `dm_indicator` VALUES ('71', 'Freezing_Shutdown', '急停开关', '3', '29', null, '1', '003');
INSERT INTO `dm_indicator` VALUES ('72', 'Modem_RSSI', 'Modem_RSSI', '3', '29', null, '1', '004');
INSERT INTO `dm_indicator` VALUES ('73', 'IfConn', '网络状态', '3', '30', null, '1', '005');
INSERT INTO `dm_indicator` VALUES ('74', 'Oil', '油', '1', '21', null, '1', null);
INSERT INTO `dm_indicator` VALUES ('75', 'no3ecolab', '硝酸盐（eco）', '5', '21', null, '1', '011');
INSERT INTO `dm_indicator` VALUES ('76', 'po4ecolab', '磷酸盐（eco）', '5', '21', null, '1', '014');
INSERT INTO `dm_indicator` VALUES ('77', 'nh4ecolab', '氨氮（eco）', '5', '21', null, '1', '013');
INSERT INTO `dm_indicator` VALUES ('78', 'no2ecolab', '亚硝酸盐（eco）', '5', '21', null, '1', '012');
INSERT INTO `dm_indicator` VALUES ('79', 'NO_2', '亚硝酸（stan）', '6', '21', null, '1', '022');
INSERT INTO `dm_indicator` VALUES ('80', 'NO_3', '硝酸盐（stan）', '6', '21', null, '1', '021');
INSERT INTO `dm_indicator` VALUES ('81', 'NH_3', '氨氮（stan）', '6', '21', null, '1', '023');
INSERT INTO `dm_indicator` VALUES ('82', 'PO_4', '磷酸盐（stan）', '6', '21', null, '1', '025');
INSERT INTO `dm_indicator` VALUES ('83', 'EH_NH4', '氨氮(E+H)', '7', '21', null, '1', '021');
INSERT INTO `dm_indicator` VALUES ('84', 'EH_Turbidity', '浊度', '7', '16', null, '1', '022');
INSERT INTO `dm_indicator` VALUES ('85', 'EH_Conductivity', '电导率', '7', '38', null, '1', '023');
INSERT INTO `dm_indicator` VALUES ('86', 'EH_DO', '溶解氧', '7', '14', null, '1', '024');
INSERT INTO `dm_indicator` VALUES ('87', 'EH_PH', 'pH', '7', '4', null, '1', '025');
