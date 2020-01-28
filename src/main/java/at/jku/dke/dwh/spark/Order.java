package at.jku.dke.dwh.spark;

import java.sql.Timestamp;

public class Order {
  private Timestamp timestamp;
  private int id;
  private int clientId;
  private String stockSymbol;
  private int stocksSold;
  private double price;
  private String action;
  
  public Order() {
    
  }
  
  public Order(Timestamp timestamp, int id, int clientId, String stockSymbol, int stocksSold, double price, String action) {
    super();
    this.timestamp = timestamp;
    this.id = id;
    this.clientId = clientId;
    this.stockSymbol = stockSymbol;
    this.stocksSold = stocksSold;
    this.price = price;
    this.action = action;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }
  public void setTimestamp(Timestamp timestamp) {
    this.timestamp = timestamp;
  }
  public int getId() {
    return id;
  }
  public void setId(int id) {
    this.id = id;
  }
  public int getClientId() {
    return clientId;
  }
  public void setClientId(int clientId) {
    this.clientId = clientId;
  }
  public String getStockSymbol() {
    return stockSymbol;
  }
  public void setStockSymbol(String stockSymbol) {
    this.stockSymbol = stockSymbol;
  }
  public int getStocksSold() {
    return stocksSold;
  }
  public void setStocksSold(int stocksSold) {
    this.stocksSold = stocksSold;
  }
  public double getPrice() {
    return price;
  }
  public void setPrice(double price) {
    this.price = price;
  }
  public String getAction() {
    return action;
  }
  public void setAction(String action) {
    this.action = action;
  }
}
