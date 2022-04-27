package practice1;

import java.io.Serializable;

public class UserData implements Serializable {
    String dateTime;
    Long userId;
    String sessionID;
    Long pageId;
    String actionTime;
    String keyWord;
    Long clickCategoryId;//某一个商品品类的 ID
    Long clickProductId;//某一个商品的 ID
    String orderCategoryIds; //一次订单中所有品类的 ID 集合
    String orderProductIds;//一次订单中所有商品的 ID 集合
    String payCategoryIds;//一次支付中所有品类的 ID 集合
    String pay_product_ids;//一次支付中所有商品的 ID 集合
    Long cityId; //城市 id

    public UserData(){

    }

    public UserData(String dateTime, Long userId, String sessionID, Long pageId, String actionTime, String keyWord,
                    Long clickCategoryId, Long clickProductId, String orderCategoryIds, String orderProductIds,
                    String payCategoryIds, String pay_product_ids, Long cityId) {
        this.dateTime = dateTime;
        this.userId = userId;
        this.sessionID = sessionID;
        this.pageId = pageId;
        this.actionTime = actionTime;
        this.keyWord = keyWord;
        this.clickCategoryId = clickCategoryId;
        this.clickProductId = clickProductId;
        this.orderCategoryIds = orderCategoryIds;
        this.orderProductIds = orderProductIds;
        this.payCategoryIds = payCategoryIds;
        this.pay_product_ids = pay_product_ids;
        this.cityId = cityId;
    }

    public String getDateTime() {
        return dateTime;
    }

    public Long getUserId() {
        return userId;
    }

    public String getSessionID() {
        return sessionID;
    }

    public Long getPageId() {
        return pageId;
    }

    public String getActionTime() {
        return actionTime;
    }

    public String getKeyWord() {
        return keyWord;
    }

    public Long getClickCategoryId() {
        return clickCategoryId;
    }

    public Long getClickProductId() {
        return clickProductId;
    }

    public String getOrderCategoryIds() {
        return orderCategoryIds;
    }

    public String getOrderProductIds() {
        return orderProductIds;
    }

    public String getPayCategoryIds() {
        return payCategoryIds;
    }

    public String getPay_product_ids() {
        return pay_product_ids;
    }

    public Long getCityId() {
        return cityId;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public void setSessionID(String sessionID) {
        this.sessionID = sessionID;
    }

    public void setPageId(Long pageId) {
        this.pageId = pageId;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public void setClickCategoryId(Long clickCategoryId) {
        this.clickCategoryId = clickCategoryId;
    }

    public void setClickProductId(Long clickProductId) {
        this.clickProductId = clickProductId;
    }

    public void setOrderCategoryIds(String orderCategoryIds) {
        this.orderCategoryIds = orderCategoryIds;
    }

    public void setOrderProductIds(String orderProductIds) {
        this.orderProductIds = orderProductIds;
    }

    public void setPayCategoryIds(String payCategoryIds) {
        this.payCategoryIds = payCategoryIds;
    }

    public void setPay_product_ids(String pay_product_ids) {
        this.pay_product_ids = pay_product_ids;
    }

    public void setCityId(Long cityId) {
        this.cityId = cityId;
    }
}
