package sunyu.util.test.entity;

import sunyu.util.annotation.Column;

import java.time.LocalDateTime;

public class SimInfo {
    @Column(column = "create_time", desc = "创建时间")
    private LocalDateTime createTime;

    @Column(column = "update_time", desc = "更新时间")
    private LocalDateTime updateTime;

    @Column(column = "sim_iccid", desc = "ICCID")
    private String simIccid;

    @Column(column = "sim_msisdn", desc = "SIM卡号")
    private String simMsisdn;

    @Column(column = "device_id", desc = "终端编号")
    private String deviceId;

    @Column(column = "device_customer_name", desc = "客户名称")
    private String deviceCustomerName;

    @Column(column = "device_shipping_time", desc = "发货时间")
    private LocalDateTime deviceShippingTime;

    @Column(column = "sim_status", desc = "SIM卡状态")
    private String simStatus;

    @Column(column = "sim_status_raw", desc = "运营商卡状态")
    private String simStatusRaw;

    @Column(column = "sim_activation_time", desc = "SIM卡激活时间")
    private LocalDateTime simActivationTime;

    @Column(column = "sim_deactivation_period", desc = "SIM卡注销时间")
    private LocalDateTime simDeactivationPeriod;

    @Column(column = "service_expiration_time", desc = "服务到期时间")
    private LocalDateTime serviceExpirationTime;

    @Column(column = "sim_operator", desc = "运营商")
    private String simOperator;

    @Column(column = "sim_plan", desc = "SIM卡套餐")
    private String simPlan;

    @Column(column = "sim_price", desc = "SIM卡单价")
    private String simPrice;

    @Column(column = "sim_purchase_time", desc = "SIM卡购买时间")
    private LocalDateTime simPurchaseTime;

    @Column(column = "device_imei", desc = "IMEI")
    private String deviceImei;

    @Column(column = "device_model", desc = "终端型号")
    private String deviceModel;

    @Column(column = "device_type", desc = "终端类型")
    private String deviceType;

    @Column(column = "device_protocol", desc = "通讯协议")
    private String deviceProtocol;

    @Column(column = "device_working_condition_id", desc = "工况ID")
    private String deviceWorkingConditionId;

    @Column(column = "device_logistics_no", desc = "物流单号")
    private String deviceLogisticsNo;

    @Column(column = "api_type", desc = "卡商")
    private String apiType;

    @Column(column = "api_sim_status_update_time", desc = "SIM卡状态最后同步时间")
    private LocalDateTime apiSimStatusUpdateTime;

    @Column(column = "api_sim_traffic_update_time", desc = "SIM卡流量最后同步时间")
    private LocalDateTime apiSimTrafficUpdateTime;

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
    }

    public String getSimIccid() {
        return simIccid;
    }

    public void setSimIccid(String simIccid) {
        this.simIccid = simIccid;
    }

    public String getSimMsisdn() {
        return simMsisdn;
    }

    public void setSimMsisdn(String simMsisdn) {
        this.simMsisdn = simMsisdn;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getDeviceCustomerName() {
        return deviceCustomerName;
    }

    public void setDeviceCustomerName(String deviceCustomerName) {
        this.deviceCustomerName = deviceCustomerName;
    }

    public LocalDateTime getDeviceShippingTime() {
        return deviceShippingTime;
    }

    public void setDeviceShippingTime(LocalDateTime deviceShippingTime) {
        this.deviceShippingTime = deviceShippingTime;
    }

    public String getSimStatus() {
        return simStatus;
    }

    public void setSimStatus(String simStatus) {
        this.simStatus = simStatus;
    }

    public String getSimStatusRaw() {
        return simStatusRaw;
    }

    public void setSimStatusRaw(String simStatusRaw) {
        this.simStatusRaw = simStatusRaw;
    }

    public LocalDateTime getSimActivationTime() {
        return simActivationTime;
    }

    public void setSimActivationTime(LocalDateTime simActivationTime) {
        this.simActivationTime = simActivationTime;
    }

    public LocalDateTime getSimDeactivationPeriod() {
        return simDeactivationPeriod;
    }

    public void setSimDeactivationPeriod(LocalDateTime simDeactivationPeriod) {
        this.simDeactivationPeriod = simDeactivationPeriod;
    }

    public LocalDateTime getServiceExpirationTime() {
        return serviceExpirationTime;
    }

    public void setServiceExpirationTime(LocalDateTime serviceExpirationTime) {
        this.serviceExpirationTime = serviceExpirationTime;
    }

    public String getSimOperator() {
        return simOperator;
    }

    public void setSimOperator(String simOperator) {
        this.simOperator = simOperator;
    }

    public String getSimPlan() {
        return simPlan;
    }

    public void setSimPlan(String simPlan) {
        this.simPlan = simPlan;
    }

    public String getSimPrice() {
        return simPrice;
    }

    public void setSimPrice(String simPrice) {
        this.simPrice = simPrice;
    }

    public LocalDateTime getSimPurchaseTime() {
        return simPurchaseTime;
    }

    public void setSimPurchaseTime(LocalDateTime simPurchaseTime) {
        this.simPurchaseTime = simPurchaseTime;
    }

    public String getDeviceImei() {
        return deviceImei;
    }

    public void setDeviceImei(String deviceImei) {
        this.deviceImei = deviceImei;
    }

    public String getDeviceModel() {
        return deviceModel;
    }

    public void setDeviceModel(String deviceModel) {
        this.deviceModel = deviceModel;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getDeviceProtocol() {
        return deviceProtocol;
    }

    public void setDeviceProtocol(String deviceProtocol) {
        this.deviceProtocol = deviceProtocol;
    }

    public String getDeviceWorkingConditionId() {
        return deviceWorkingConditionId;
    }

    public void setDeviceWorkingConditionId(String deviceWorkingConditionId) {
        this.deviceWorkingConditionId = deviceWorkingConditionId;
    }

    public String getDeviceLogisticsNo() {
        return deviceLogisticsNo;
    }

    public void setDeviceLogisticsNo(String deviceLogisticsNo) {
        this.deviceLogisticsNo = deviceLogisticsNo;
    }

    public String getApiType() {
        return apiType;
    }

    public void setApiType(String apiType) {
        this.apiType = apiType;
    }

    public LocalDateTime getApiSimStatusUpdateTime() {
        return apiSimStatusUpdateTime;
    }

    public void setApiSimStatusUpdateTime(LocalDateTime apiSimStatusUpdateTime) {
        this.apiSimStatusUpdateTime = apiSimStatusUpdateTime;
    }

    public LocalDateTime getApiSimTrafficUpdateTime() {
        return apiSimTrafficUpdateTime;
    }

    public void setApiSimTrafficUpdateTime(LocalDateTime apiSimTrafficUpdateTime) {
        this.apiSimTrafficUpdateTime = apiSimTrafficUpdateTime;
    }
}