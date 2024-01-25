---
title: "Prefect Cloudでワークフロー構築時のDX向上させたはなし"
emoji: "♻"
type: "tech" # tech: 技術記事 / idea: アイデア
topics: ["Prefect", "Workflows", "dataengineering", "Python"]
published: false
publication_name: overflow_offers
---

# はじめに

おはこんにちは。

[Offers](https://offers.jp/) と、[Offers MGR](https://offers-mgr.com/lp/) を運営している株式会社 [overflow](https://overflow.co.jp/) のバックエンドエンジニアばばです。

Web サービスを開発している皆様であれば、データを扱うことの大変さやつらさを味わったことはあるかと思います。

- この表示用データを作成するには、API からのデータ取得が終わった後に、DWH で集計して・・・
- 想定してたよりデータ量が多くて、許容時間内に処理が終わらない
- あの API の Rate Limit 厳しいからから並列数制御しないと

など、悩みはつきません（少なくとも、私は）。

弊社では、この非機能要件の制御に Sidekiq の機能を活用してどうにか運用をしてきました。

Sidekiq には、Batch を利用して複雑なワークフローを組めるのですが、 Callback 中に新しい Workflow を作るという、
依存関係を実装することが大変つらくなりました。

しかし、プロダクトの複雑度があがるほどに、データパイプラインの複雑度も上がり、ワークフローが複雑になっていきます。
Sidekiq には

今回は、 Workflow Engine を導入した（厳密にはしている最中）はなしです。


弊社サービス Offers MGR は、複数の SaaS サービスのデータを取得して、可視化することで組織の健康状態を可視化する機能を提供しています。
そのため、SaaS サービスのデータの取得および集計・加工処理することが、バックエンド側の重要な責務となっています。

プロダクトの成長により、従来は単純なバッチ処理でよかったものが、複雑なデータパイプラインを要求する段階となってきました。
今回は、


# Prefect Cloud

Prefee


