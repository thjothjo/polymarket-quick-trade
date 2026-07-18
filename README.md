# Polymarket Quick Trade & Onchain Leaderboard

一个支持 Polymarket 多市场的快捷交易面板，并集成实时的链上盈亏与大户交易排名榜单。

## 特性
- ⚡️ **单键快速下单**: 支持通过键盘快捷键快速进行买入/卖出
- 📊 **多时间维度市场**: 支持快速切换 BTC/ETH 的 5分钟、15分钟、1小时市场
- 🏆 **链上大户排行榜**: 实时抓取链上订单数据，统计交易排名 Top 100
- 🔄 **多账号切换**: 支持配置多个交易账号并在面板中一键切换

## 环境要求

- **Python >= 3.10**
- **Alchemy API Key** (用于 Polygon RPC 读取链上数据)
- **Polymarket 交易钱包私钥** (用于下单交易)

## 安装步骤

1. 克隆本仓库到本地：
```bash
git clone https://github.com/JamesLHW/polymarket-quick-trade.git
cd polymarket-quick-trade
```

2. 安装依赖包：
```bash
pip install -r requirements.txt
```

3. 复制配置模板并填写配置：
```bash
cp .env.template .env
```
   - 在 `.env` 里填入你的 `ALCHEMY_KEY`（可在 [alchemy.com](https://www.alchemy.com/) 免费申请）
   - 填入你的 Polymarket 交易钱包私钥 `QUICK_PRIVATE_KEY` 与对应的地址 `QUICK_FUNDER`

## 运行方法

```bash
python onchain_leaderboard.py
```

启动后终端会提示：
```text
  ⚡ Quick Trade: http://localhost:8890
```

使用浏览器打开 `http://localhost:8890` 即可使用快速交易面板和查看实时链上榜单。

## 快捷键

| 按键 | 功能 |
|------|------|
| `↑` | 买入 UP |
| `↓` | 买入 DOWN |
| `Q` | 卖出全部 UP |
| `W` | 卖出全部 DOWN |
| `1`~`5` | 选择金额 ($5 / $10 / $20 / $50 / $100) |

## 其他命令

```bash
# 只查询一次当前回合排名（不进入实时监控）
python onchain_leaderboard.py --once

# 指定回合时间戳查询历史排名
python onchain_leaderboard.py --ts 1775757000
```

## 注意事项

> ⚠️ **永远不要将 `.env` 文件提交到 Git！** 里面包含你的私钥。项目已配置 `.gitignore` 自动忽略该文件。
