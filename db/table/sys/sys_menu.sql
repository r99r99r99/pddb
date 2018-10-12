/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50625
Source Host           : localhost:3306
Source Database       : sdocean

Target Server Type    : MYSQL
Target Server Version : 50625
File Encoding         : 65001

Date: 2016-03-21 16:09:07
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for sys_menu
-- ----------------------------
DROP TABLE IF EXISTS `sys_menu`;
CREATE TABLE `sys_menu` (
  `code` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `pcode` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `type` int(11) NOT NULL DEFAULT '1' COMMENT '是否是末节点,1代表是末节点',
  `name` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `level` int(11) DEFAULT NULL COMMENT '菜单级别',
  `url` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `picture` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  `isactive` int(11) NOT NULL DEFAULT '1' COMMENT '0:禁用    1:启用',
  `ordercode` varchar(6) COLLATE utf8_bin DEFAULT '1' COMMENT '排序',
  UNIQUE KEY `un_code_menu` (`code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of sys_menu
-- ----------------------------
--菜单
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('000001', '000000', '0', '菜单', '1', null, null, '1', '0000');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('000002', '000001', '0', '展示系统', '1', null, null, '1', '0000');


-------前台管理系统菜单----------
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('600000', '000002', '0', '在线监测', '2', null, 'zaixian.jpg', '1', '0100');
--前台首页
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('600001', '600000', '0', '首页', '3', 'firstPage.do', 'icon-home', '1', '0001');


--数据查询
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('106000', '600000', '0', '数据查询', '3', '', 'icon-th-list', '1', '0510');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('106005', '106000', '1', '实时数据', '4', 'dataqueryNow_init.do', null, '1', '0511');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('106010', '106000', '1', '历史数据查询', '4', 'dataquery_init.do', null, '1', '0511');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('106060', '106000', '1', '系统状态查询', '4', 'systemQuery_init.do', null, '1', '0516');

--报表管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('112000', '600000', '0', '环境报表', '3', '', 'icon-list-alt', '1', '0580');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('112010', '112000', '1', '日报上报', '4', 'dailyReport_info.do', null, '1', '0001');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('112020', '112000', '1', '月报上报', '4', 'monthReport_info.do', null, '1', '0002');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('112030', '112000', '1', '报表查询', '4', 'reportQuery_info.do', null, '1', '0003');

--站点信息
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('113000', '600000', '0', '站点信息', '3', '', 'icon-star', '1', '1300');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('113010', '113000', '1', '图片档案', '4', 'stationPic_info.do', null, '1', '0001');


--数据评价
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('107000', '600000', '0', '数据评价', '3', null, 'icon-question-sign', '1', '0520');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('107010', '107000', '1', '综合查询', '4', 'info_synthquery.do', null, '1', '0521');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('107020', '107000', '1', '水质统计', '4', 'info_statisquery.do', null, '1', '0522');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('107030', '107000', '1', '综合趋势', '4', 'info_graphquery.do', null, '1', '0523');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('107050', '107000', '1', '综合对比', '4', 'info_comparison.do', null, '1', '0525');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('107055', '107000', '1', '综合报表', '4', 'info_statisreportavg.do', null, '1', '0525');
 
--预警告警
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('108000', '600000', '0', '预警告警', '3', null, 'icon-warning-sign', '1', '0530');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('108020', '108000', '1', '预警告警信息', '4', 'info_warnvalue.do', null, '1', '0532');

--通知管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('170000', '600000', '0', '通知管理', '3', null, 'icon-envelope', '1', '0540');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('170010', '170000', '1', '通知上报', '4', 'info_noticeUp.do', null, '1', '0901');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('170020', '170000', '1', '通知下发', '4', 'info_noticeDown.do', null, '1', '0902');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('170030', '170000', '1', '通知查询', '4', 'info_noticeList.do', null, '1', '0903');


--维护管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('130000', '600000', '0', '维护管理', '3', null, 'icon-wrench', '1', '0570');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('102050', '130000', '1', '站点维护查询', '4', 'info_mainShow.do', null, '1', '0125');


-------------------海水浴场
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('200000', '000002', '0', '海水浴场', '3', null, 'yuchang.jpg', '1', '0580');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('200001', '200000', '0', '首页', '3', 'info_beach.do', 'icon-home', '1', '0001');
--数据管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('201000', '200000', '0', '数据管理', '3', '', 'icon-th-list', '1', '0002');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('201010', '201000', '1', '数据上报', '4', 'info_beachImport.do', null, '1', '0001');
--数据评价
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('202000', '200000', '0', '数据评价', '3', null, 'icon-question-sign', '1', '0003');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('202010', '202000', '1', '游泳指数统计', '4', 'info_beachstatisquery.do', null, '1', '0001');


-------------------增养殖区
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('300000', '000002', '0', '增养殖区', '3', null, 'yangzhi.jpg', '1', '0590');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('300001', '300000', '0', '首页', '3', 'info_aquacu.do', 'icon-home', '1', '0001');


-----海水增养殖区数据评价
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('302000', '300000', '0', '数据评价', '3', null, 'icon-question-sign', '1', '0003');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('302010', '302000', '1', '增养殖区水质统计', '4', 'info_redStatisQuery.do', null, '1', '0001');


-------------------赤潮孕宰预警
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('400000', '000002', '0', '赤潮孕宰预警', '3', null, 'chichao.jpg', '1', '0560');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('400001', '400000', '0', '首页', '3', 'info_redTide.do', 'icon-home', '1', '0001');

--赤潮数据评价
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('402000', '400000', '0', '数据评价', '3', null, 'icon-question-sign', '1', '0003');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('402010', '402000', '1', '赤潮监控区域水质统计', '4', 'info_redStatisQuery.do', null, '1', '0001');


----------------后台管理系统菜单-----------------------
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('100000', '000001', '0', '管理系统', '2', null, null, '1', '0500');
--系统管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('101000', '100000', '0', '系统管理', '3', null, 'fa fa-th-large', '1', '0110');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('101010', '101000', '1', '人员管理', '4', 'info_sysUser.do', null, '1', '0111');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('101020', '101000', '1', '角色管理', '4', 'info_sysRole.do', null, '1', '0112');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('101040', '101000', '1', '功能区划管理', '4', 'info_company.do', null, '1', '0113');
--INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('101050', '101000', '1', '公司管理', '4', 'info_company.do', null, '1', '0114');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('101060', '101000', '1', '系统登录日志', '4', 'info_sysLogin.do', null, '1', '0114');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('101070', '101000', '1', '用户操作日志', '4', 'info_sysOperation.do', null, '1', '0115');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('101080', '101000', '1', '系统状态日志', '4', 'info_sysLog.do', null, '1', '0116');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('101090', '101000', '1', '系统设置', '4', 'info_sysMap.do', null, '1', '0117');


--站点管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('102000', '100000', '0', '站点信息', '3', null, 'fa fa-map-marker', '1', '0620');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('102010', '102000', '1', '站点类型管理', '4', 'info_station_type.do', null, '1', '0121');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('102020', '102000', '1', '站点管理', '4', 'info_station.do', null, '1', '0122');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('102100', '102000', '1', '站点设备配置', '4', 'info_stationDevice.do', null, '1', '0122');

INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('102030', '102000', '1', '站点配置', '4', 'info_stationComm.do', null, '1', '0123');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('102080', '102000', '1', '站点选择参数', '4', 'info_stationIndicator.do', null, '1', '0123');

INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('102060', '102000', '1', '站点信息维护', '4', 'info_stationInfo.do', null, '1', '0126');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('102070', '102000', '1', '站点信息展示', '4', 'info_stationShow.do', null, '1', '0127');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('102090', '102000', '1', '站点设备展示', '4', 'info_stationDeviceShow.do', null, '1', '0128');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('102091', '102000', '1', '站点图片档案', '4', 'info_stationPicture.do', null, '1', '0129');

--字典管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('103000', '100000', '0', '字典管理', '3', null, 'fa fa-book', '1', '0130');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('103010', '103000', '1', '单位组管理', '4', 'info_unit_group.do', null, '1', '0131');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('103020', '103000', '1', '单位管理', '4', 'info_unit.do', null, '1', '0132');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('103030', '103000', '1', '公共代码管理', '4', 'info_public.do', null, '1', '0133');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('103040', '103000', '1', '上传文件管理', '4', 'info_file.do', null, '1', '0134');

--参数管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('104000', '100000', '0', '参数管理', '3', null, 'fa fa-chain', '1', '0140');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('104010', '104000', '1', '参数组管理', '4', 'info_indicator_group.do', null, '1', '0141');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('104020', '104000', '1', '参数管理', '4', 'info_indicator.do', null, '1', '0142');

--设备管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('105000', '100000', '0', '设备管理', '3', null, 'fa fa-gear', '1', '0150');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('105010', '105000', '1', '设备管理', '4', 'info_device.do', null, '1', '0151');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('105020', '105000', '1', '水质等级管理', '4', 'info_water_level.do', null, '1', '0134');


--短信管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('109000', '100000', '0', '短信管理', '3', null, 'fa fa-envelope', '1', '0540');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('109010', '109000', '1', '短信发送策略', '4', 'info_smssetting.do', '', '1', '0541');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('109020', '109000', '1', '短信模板配置', '4', 'info_smsmould.do', '', '1', '0542');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('109030', '109000', '1', '短信发送记录', '4', 'info_smslog.do', '', '1', '0543');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('109040', '109000', '1', '短信发送配置', '4', 'info_smsstation.do', '', '1', '0541');

--首页管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('110000', '100000', '0', '首页管理', '3', null, 'fa fa-home', '1', '0550');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('101030', '110000', '1', '首页实时数据', '4', 'info_firstpageshow.do', null, '1', '0113');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('111000', '110000', '1', '首页系统状态', '4', 'info_firstpagesystem.do', null, '1', '0114');

--入海污染量
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('120000', '100000', '0', '入海污染量', '3', null, 'fa fa-flask', '1', '0560');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('121000', '120000', '1', '入海污染量查询', '4', 'info_pollutant.do', null, '1', '0561');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('122000', '120000', '1', '入海污染量设置', '4', 'info_pollutantset.do', null, '1', '0562');


--功能区管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('131000', '100000', '0', '功能区管理', '3', null, 'fa fa-plus-square', '1', '0580');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('131010', '131000', '1', '功能区管理', '4', 'info_domain.do', null, '1', '0001');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('132010', '131000', '1', '功能区阈值', '4', 'info_domainThreshold.do', null, '1', '0002');


--数据管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('140000', '100000', '0', '数据管理', '3', '', 'fa fa-tasks', '1', '0510');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('106020', '140000', '1', '数据管理', '4', 'dataChange_init.do', null, '1', '0512');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('106030', '140000', '1', '数据导入', '4', 'dataImport_init.do', null, '1', '0513');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('106040', '140000', '1', '数据更改日志', '4', 'dataChangeLog_init.do', null, '1', '0514');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('107040', '140000', '1', '评价模板', '4', 'info_consetting.do', null, '1', '0524');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('106050', '140000', '1', '手动数据导入', '4', 'manualDataImport_init.do', null, '1', '0524');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('140010', '140000', '1', '数据审核', '4', 'dataCheck_init.do', null, '1', '0512');

--预警告警管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('150000', '100000', '0', '预警告警', '3', null, 'fa fa-warning', '1', '0530');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('108010', '150000', '1', '预警告警配置', '4', 'info_warnsetting.do', null, '1', '0531');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('108030', '150000', '1', '设备报警码', '4', 'info_devicealarm.do', null, '1', '0534');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('108040', '150000', '1', '设备量程配置', '4', 'info_rangevalue.do', null, '1', '0533');

--维护管理
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('160000', '100000', '0', '维护管理', '3', null, 'fa fa-wrench', '1', '0570');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('102040', '160000', '1', '站点维护上报', '4', 'info_mainEdit.do', null, '1', '0124');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('130040', '160000', '1', '例行维护类型', '4', 'info_MainConfig.do', null, '1', '0000');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('130010', '160000', '1', '设备维护配置', '4', 'info_stationDeviceMain.do', null, '1', '0001');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('130020', '160000', '1', '站点异常上报', '4', 'info_errorEdit.do', null, '1', '0126');
INSERT INTO sys_menu(code,pcode,type,name,level,url,picture,isactive,ordercode) VALUES ('130030', '160000', '1', '站点异常查询', '4', 'info_errorShow.do', null, '1', '0127');

