/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : liaohe_oceantech

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2015-12-24 10:49:14
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for authority
-- ----------------------------
DROP TABLE IF EXISTS `authority`;
CREATE TABLE `authority` (
  `ID` varchar(32) NOT NULL,
  `NAME` varchar(255) NOT NULL COMMENT 'Ȩ������',
  `AUTHKEY` varchar(255) NOT NULL COMMENT 'Ȩ�޹ؼ���',
  `REMARK` varchar(2000) DEFAULT NULL COMMENT '��ע',
  `TYPE` decimal(8,0) DEFAULT NULL COMMENT '����',
  PRIMARY KEY (`ID`),
  KEY `AUTHORITY_U_AUTHKEY` (`AUTHKEY`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Ȩ�ޱ�';

-- ----------------------------
-- Records of authority
-- ----------------------------
INSERT INTO `authority` VALUES ('438C501A31F63D2A27151B98B62B2132', '报警设置', 'makealarm', '设置报警值', null);
INSERT INTO `authority` VALUES ('4CF8B845C8CC19C39B71EE32EC5B19AD', '报警定制', 'makeparam', '定制报警参数', null);
INSERT INTO `authority` VALUES ('4EF6E5D3FDAE5514697639912452F85A', '用户管理', 'usermanager', '管理用户', null);
INSERT INTO `authority` VALUES ('991FDDC7D1F888C1062AC43947D30269', '权限管理', 'authoritymanager', '管理权限', null);
INSERT INTO `authority` VALUES ('A3264A28C2DE60E5E3D7A66A71BECBC7', '角色管理', 'rolemanager', '管理角色', null);
INSERT INTO `authority` VALUES ('AFF42A17678A1DD528E89C153C17F017', '主页', 'zhuye', '主页实时查看数据信息', null);
INSERT INTO `authority` VALUES ('B75F364D54D82FC5769448EFA9BC4166', '报警管理', 'alarmmenu', '报警菜单', null);
INSERT INTO `authority` VALUES ('D21A3044499B7BEA87E1E0EB68B055A4', '站点管理', 'stationmanager', '管理站点', null);
INSERT INTO `authority` VALUES ('D34510F6A604AD0027DEAC6AF6F9A2C8', '系统管理', 'systemmenu', '菜单', null);
INSERT INTO `authority` VALUES ('EB2F1DB24232F9E517646C500A028854', '软件测试', '测试', '软件测试', null);
INSERT INTO `authority` VALUES ('F795E1E5AF810975ECC684DDDD635718', '数据分析', 'viewdata', '查看历史数据趋势图和历史数据', null);
