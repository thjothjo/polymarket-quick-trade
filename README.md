# Polymarket Quick Trade & Onchain Leaderboard

一个支持 Polymarket 多市场的快捷交易面板，并集成实时的链上盈亏与大户交易排名榜单。

## 特性
- ⚡️ **单键快速下单**: 支持通过按键快速进行买入/卖出。
- 📊 **多时间维度市场**: 支持快速切换 5分钟、15分钟、1小时 市场。
- 🏆 **链上大户排行榜**: 实时抓取链上订单数据，统计交易排名。

## 安装步骤

1. 克隆本仓库到本地。
2. 安装依赖包：
```bash
pip install -r requirements.txt
```
3. 复制配置模板并填写配置：
```bash
cp .env.template .env
```
   - 申请并在 `.env` 里填入你的 `ALCHEMY_KEY`
   - 填入你的 Polymarket 交易钱包私钥 `QUICK_PRIVATE_KEY` 与对应的地址 `QUICK_FUNDER`

## 运行方法

在项目目录下执行以下命令启动服务：

```bash
python onchain_leaderboard.py
```
终端会出现如下提示：
```text
  ⚡ Quick Trade: http://localhost:8890
```
使用浏览器打开 `http://localhost:8890` 即可使用快速交易面板和查看实时链上榜单。
