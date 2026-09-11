# 從第一原理理解開源專案

[English](README.md) | [繁體中文](README.zh-TW.md)

多數 onboarding 教材從 API 與術語開始。本 repository 改從物理限制、失敗模式、不變量與設計取捨出發，再把這些概念連到 source code、test、issue 與真實的貢獻流程。

目標是協助具備技術基礎的新手走向 **Contribution Ready**，而不只是學會 API。

## 學習路線

| Track | zh-TW | English | 狀態 |
| --- | --- | --- | --- |
| Kafka | [閱讀](docs/kafka/zh-TW/README.md) | [Read](docs/kafka/en/README.md) | 審閱用原型 |
| Ray | [規劃中](docs/ray/zh-TW/README.md) | [Planned](docs/ray/en/README.md) | Planned |

未來的 infrastructure tracks 可以沿用 `docs/` 下相同的語言與內容結構。

## 教學方法

```text
問題
→ 物理限制
→ 設計選擇
→ 機制
→ 保證
→ 不保證
→ 失敗邊界
→ Test / Source / Issue
```

每一條 track 都維持簡短的學習主線，同時把有邊界的 guarantee 連到 contributor 所需的 upstream evidence。

## 方法論來源

本 repository 的教學方法，源自 **Yen-Hua Chen** 在 **Streaming System + Compass** 中長期記錄的工程推理方式。

Kafka track 並不是單純要求 AI 生成一份 Kafka 入門教材，而是先：

1. 從 Compass 的 ADR、postmortem、reasoning notes 與設計哲學中抽取反覆出現的推理模式；
2. 再以 Apache Kafka 官方文件、Javadoc、KIP、source code 與 tests 獨立驗證這些推理是否真的適用；
3. 排除物理機制不同、容易造成誤導的類比；
4. 最後才把留下來的 reasoning patterns 整理成 first-principles onboarding path。

這不是在主張 Kafka 的設計源自 Compass，而是明確記錄**這套教材 framing 與教學方法的來源**。

方法論原始來源：[https://github.com/hikaru-212/streaming-system-compass](https://github.com/hikaru-212/streaming-system-compass)

## 授權

文件與教育性文字採用 [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)；軟體與具實質內容、可獨立執行的 examples 採用 [Apache License 2.0](LICENSE)。文件內的簡短示意程式碼仍是該 CC BY 4.0 文件的一部分。

完整授權範圍請見 [LICENSE-CONTENT.md](LICENSE-CONTENT.md)。

## 語言

本頁是 `zh-TW` 繁體中文首頁。英文首頁請見 [README.md](README.md)。
