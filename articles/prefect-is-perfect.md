---
title: "Prefect Cloudでワークフロー構築時のDX向上させようとしているはなし"
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

上記の悩みが、開発効率性を落とし、技術的負債となってきていることから、Workflow Engine (タスクの実行順序など業務プロセスを自動で管理するソフトウエア) を導入しようという話がチーム内でおこりました。

今回は、Workflow Engine をどのように選定したか、そして、現在導入を進めている [Prefect](https://www.prefect.io/) ついてご紹介いたします。

# 現在のデータ処理基盤のつらみ

弊社のバックエンド技術スタックは Ruby on Rails で統一されており、この非機能要件の制御に Sidekiq の機能を活用して運用しています。
データの取得・集計などの基盤も、Sidekiq Worker 上でマイクロバッチ処理を実装し、[Sidekiq Pro Batches and Callbacks](https://www.youtube.com/watch?v=b2fI0vGf3Bo&list=PLjeHh2LSCFrWGT5uVjUuFKAcrcj5kSai1) (以下、 Sidekiq Batches) を利用しています。
Sidekiq Worker 自体は、API の非同期処理ユースケースからわかるように、分散処理には強く、きめこまやかで柔軟な処理が実装できます。

また、 Rate Limit や DB 負荷の問題に対応する流量制限には、Sidekiq Enterprise の [Rate Limiting](https://github.com/sidekiq/sidekiq/wiki/Ent-Rate-Limiting) を活用し制限しています。

Ruby on Rails の資源を利用しつつ、　カジュアルに Workflow を組めるのは大変便利なので重宝していますが、プロダクトに求められる要件の複雑化により、Workflow も複雑化していきました。
その実装を進める上で、様々な痛みが起こるようになりました。

## Sideiq Batches での Workflow を組むと可読性が低く、Testing も難しい

Sidekiq Batches は、Batch と呼ばれるグループ化した Worker 群の完了を Sidekiq Server 側で管理し、完了したら Callbacks を起動する仕組みになっています。
なので、依存関係を表現は、「Callbacks 中で次の Batch を作成して実行する」となります。
これが、Flow の直感的な部分を阻害しています。

Workflow は宣言的、手続的にかけたわかりやすくなります。理想は、こんな感じです。
GitHub Actions はその最たるもので YAML を使うことでわかりやすい構成になっています。

```
flow1-jobs:
  name: flow1
  steps:
    - run: flow1-task1
    - run: flow1-task2

flow2-jobs:
  name: flow2
  steps:
    - run: flow2-task

flow3:
  needs: [flow1-jobs, flow2-jobs]
  steps:
    - run: flow3-task
flow4:
  needs: [flow3]
  steps:
    - run: task4-1
    - run: task4-2
```

ですが、これを Sidekiq Batches で実装すると、Sidekqi Batches の知識がないと可読性の低いコードになります。
(この疑似コードの実装でも batch_id の扱いにかなり混乱します)

データの受け渡しは、Redis や DB を使わない限り、引数の `arguments` で受け渡す必要があります。
単体テストするにも Redis が必要になり、Sidekiq 上で動作させると意図しない動きをすることもあります。ふぅ。


```
# Workflow のエントリポイント
def start_workflow(arguments)
  overall = Sidekiq::Batch.new
  overall.on(:success, 'OverallCallback#finished', arguments)
  overall.jobs do
    StartWorkflow.perform_async(arguments)
  end
end

class StartWorkflow
   include Sidekiq::Job
   def perform (arguments)
      batch.jobs do
        step1 = Sidekiq::Batch.new
        step1.on(:success, "OverflowCallback#step1_done", arguments)
        steps1.jobs do
          Flow1Job.perform_async(arguments)
          Flow2Job.perform_async(arguments)
        end
      end
   end
end

def OverallCallback
  def step1_done(status, options)
    overall = Sidekiq::Batch.new(status.parent_bid)
    overall.jobs do
      batch.jobs do
        step2 = Sidekiq::Batch.new
        step2.on(:success, "OverflowCallback#step2_done", arguments)
        step2.jobs do
          Flow3Job.perform_async(arguments)
        end
      end
    end
  end

  def step2_done(status, options)
    overall = Sidekiq::Batch.new(status.parent_bid)
    overall.jobs do
      batch.jobs do
        step3 = Sidekiq::Batch.new
        step3.jobs do
          Flow4Job.perform_async(arguments)
        end
      end
    end
  end

  def finished(status, options)
    overall = Sidekiq::Batch.new(status.parent_bid)
    overall.jobs do
      batch.jobs do
        step3 = Sidekiq::Batch.new
        step3.jobs do
          Flow4Job.perform_async(arguments)
        end
      end
    end
  end
end

class Flow1Job
  include Sidekiq::Job

  def perform(arguments)
    batch.jobs do
      step1 = Sidekiq::Batch.new
      step1.on(:success, "Flow1JobCallback#step1_callbacks", arguments)
      step1.jobs do
        Flow1Task1.perform_async(arguments)
      end
    end
  end
end

class Flow1JobCallback
  def step1_callbacks(status, options)
    overall = Sidekiq::Batch.new(status.parent_bid)
    overall.jobs do
      step2 = Sidekiq::Batch.new
      step2.jobs do
        Flow1Task2.perform_async(arguments)
      end
    end
  end
end

class Flow2Jobs
  include Sidekiq::Job

  def preform(arguments)
    batch.jobs do
      step1 = Sidekiq::Batch.new
      step1.jos do
        Flow2Task.perform_async(arguments)
      end
    end
  end
end

class Flow3Jobs
  include Sidekiq::Job

  def preform(arguments)
    batch.jobs do
      step1 = Sidekiq::Batch
      step1.on(:sucess, "Flow3JobCallbacks#step1_callbacks", arguments)
      step1.jobs do
        Flow3Task.perform_async(arguments)
      end
    end
  end
end

class Flow3JobCallback
  def step1_callbacks(status, options)
     overall = Sidekiq::Batch.new(status.parent_bid)
     overall.jobs do
       step2 = Sidekiq::Batch.new
       step2.jobs do
         Flow3Task2.perform_async(options)
       end
     end
  end
end

class Flow4Jobs
  include Sidekiq::Job

  def preform(arguments)
    child = Sidekiq::Batch
    child.on(:sucess, "Flow4JobCallbacks#step1_callbacks", arguments)
    child.jobs do
      Flow4Task1.perform_async(arguments)
    end
  end
end

class Flow4JobCallback
  def step1_callbacks(status, options)
     batch = Sidekiq::Batch.new(status.parent_bid)
     batch.jobs do
       Flow4Task2.perform_async(options)
     end
  end
end

```

## もっと楽にわかりやすい Workflow を記述したい

このように Sidekiq Batch では、Workflow の管理のためのコスト、および、非同期処理のデバッグのしにくいデメリットがありました。
そして、そのデメリットは、プロダクトの開発工数にも影響を与えるようになってきました。

本来であれば、 Workflow の管理ではなく、データ集計ロジックなどプロダクトの価値をあたえるものに時間を注ぐべきです。

そのため、複数の Workflow Engine の導入を検討し、比較することになりました。

# Workflow Engine の検討

Workflow Engine には様々なプロダクトがあります。

## 選定基準

基本的な Workflow Engine の機能要件・性能要件を満たしていることは最低限の条件でした。

- DAG のような、依存関係をもった実行順が管理できること
- スケジュール機能があること
- フロー単位での停止やリトライができること

また、以下のような連携機能を有することも重視しました。

- 弊社特有の事象として、Sidekiq や AWS と親和性高く連携できるか
- AWS との連携や拡張性が楽で、極力マネージドな構成と Open Source が選べるもの
- Developer EXperience をあげてくれるもの

### DAG とは？

GitHub Workflow のような Workflow Engine でよく見られるデータ構造のことを、
[向き非巡回グラフ](https://ja.wikipedia.org/wiki/%E6%9C%89%E5%90%91%E9%9D%9E%E5%B7%A1%E5%9B%9E%E3%82%B0%E3%83%A9%E3%83%95) (DAG) と呼びます。

Workflow Engine の多数は、DAG を表現できるインタフェース (YAML, コードなど) を持っているか、プログラム内部で DAG を作成します。
また、DAG に対応した Workflow Engine はその複雑な依存関係から、図による可視化機能を備えています。
そのため、依存関係や実行順を直感的に管理できます。

## 候補の洗い出し

有限の時間の中から以下をピックアップして検討し、点数をつけて評価しました。
もちろんすべてを実際に使用して評価したのではなく、ドキュメントやブログなどの情報のみにとどめたものがあります。

候補として有力視されたのは以下の 4 つです。

### [Airflow](https://airflow.apache.org/)

Python でフローを記述する
オープンソース
有名なプロダクトで AWS 上にもマネージドサービス (AWS MWAA) として展開されている
エンプラ向け

### [Digdag](https://www.digdag.io/)

YAML でフローを記述する
オープンソース
Treasure Data 社により、 embulk などとの親和性が高い
小〜中規模向け

### [Prefect](https://www.prefect.io/)

Python でフローを記述する
オープンソース
後発であり、ML / DataEnginee 向けで、Airflow の不満点を補った作りになっている
小〜中規模向け(エンプラもいける？)

### [Kestra](https://kestra.io/)

YAML でフローを記述する
オープンソース
後発であり、高可用性をもたせた作り
エンプラ向け?

Kuebenetes に特化した Argoflow や、クラウドベンダが提供するサービス(Step Functions)もありますが、今回は除外しました。

## 他の Workflow Engine に興味のある方へ

網羅性があります。 GitHub Stars の数も参考になりますよ。

https://github.com/meirwah/awesome-workflow-engines


## 選定

この中で、検証を進め、最終的に Prefect に決定しました。
いずれも素晴らしいプロダクトであり、最低限の要件は満たしていたため最終的には「筆者の好み」になるのかもしれません。

### Airflow

- DAG の定義が必要となり直感的ではない(動的な DAG もなかなか大変そう)
- 実行時のパラメータ設定は可能だが面倒
- (MWAA を使った場合) ランニングコストがかかってしまいスモールに開始できない

### Digdag

- マネージドがなく、環境構築にコストがかかる
- 実行時にパラメータを渡すことができない (Workflow の修正が必要)
- YAML は便利だが拡張シンタックスがわかりにくい

### Kestra

- マネージドがなく、Restack を使ってもコストがかかる
- 拡張機能が Java での実装があり社内技術スタックと相性が悪い
- ECS 連携が公式のプラグインで対応していなかった

# Prefect について

Prefect Cloud のメリットを説明します。

Prefect 側も Airflow との対比記事を出しているので、参考までにご覧ください。

https://www.prefect.io/prefect-vs-airflow

## Prefect のアーキテクチャーについて

Prefect のアーキテクチャはシンプルですが、柔軟性が高く、様々なデプロイ方法があるため、複雑に見えます。

ですが、肝は次の通りです:

- Flow は Python Code であり、 Python と Prefect Python SDK がインストールされていれば、原則的にはどの環境でも実行可能である
- Deployment には Worker モデルと Serve モデル、Push Work モデル(Prefect Cloud のみ対応) の 3 タイプがある
  - Worker モデルは、Flow の実行環境と分離しているモデル
  - Serve モデルは、Flow を常駐させ、実行させるモデル
  - Push Work モデルは、Prefect Cloud から実行環境上でフローを実行するモデル
- Flow の Code はストレージ上に保存される
  - Local, AWS S3, GCS, Azure のほか、 docker images に組み込んだり、 Git Repository から Pull することが可能
- リソースへのアクセス情報は Blocks として定義する
  - 例えば、 AWS クレデンシャルなど
  - コネクションを wrap することも可能
- 拡張機能は Integrations と呼ばれ、Python で記述可能

![アーキテクチャ](/images/prefect-is-perfect/img2.png)


## Prefect のメリット

### Python の環境に閉じている

Python に特化しているため、 エコシステムが利用でき、Python さえ理解していれば学習コストは低くなります。

Python 自身の学習コストが発生しますが、データサイエンスや機械学習でのメジャーな言語となることや、Ruby や Perl などの近いシンタックスをもつことから、
Web サービス開発のバックエンドエンジニアにはなじみやすいといえるでしょう。 (好みは別にして・・・)

### DAG を記述せずにタスクの依存関係を直感的に記述できる

Airflow とは異なり、DAG を意識せずに実装可能な構造となっています。

先ほどの GitHub Actions で記述したフローを Prefect のコードに落としたものです。

```
from prefect import flow, task

@task
def job1_task():
   print('do flow1-task1')
   print('do flow1-task2')
   return 'return job1'

@task
def job2_task():
   print('do job2-task')

@task
def job3_task():
   print('do job3-task')

@task
def job4_task():
   print('do job4-task1')
   print('do job4-task2')

@flow
def overall_flow(arg: str):
    job1_future = job1_task.submit()
    print('job1 submitted')
    job2_future = job2_task.submit()
    print('job2 submitted')
    result = job1_future.result()
    print(f'job1 finished. job1 result={result}')
    job2_future.wait()
    print(f'job2 finished.')

    job3_task()
    job4_task()

if __name__ == '__main__':
    overall_flow()
```

このスクリプトですが、 pip で prefect をインストールすれば、特に設定せずに実行できます！

```
python3 -mvenv .venv
source ~/.venv/bin/activate
pip install prefect

python sample.flow
```

#### コードの解説

@flow デコレータで囲まれた部分がフローを定義しています。

デフォルトの挙動では、タスクは Python メソッドのように実行できます。

```
job3_task()
job4_task()
```

上記は、 job3_task がおわった後に job4_task が実行されます。 Python の挙動と同じですね。

job1_task と job2_task は並列実行させたいので、その場合は `submit()` を使います。

`submit()` を呼び出すと、現在のタスクランナーに送信され、非同期で実行されます。

非同期関数は、 `wait()` で待機します。
`result()` を使うことで、待機して戻り値を取得できます。

もちろん、次のタスクに値を渡すことができます！

### フローとスケジュール（デプロイメント）が分離している

Prefect もスケジュール実行をする場合は、Prefect サーバ上に、登録する必要があります。

これ Prefect では `デプロイメント` と呼び、フローの実装とは独立しています。
フローとスケジュールなどの定義が分離されていることから、環境毎に実行時間や実行環境を変えたい、ことを柔軟に行えます。

デプロイメントは、 prefect.yaml と呼ばれる設定ファイルに、スケジュールや Work Pool という実行環境などを設定します。

たとえば、のフロー定義を　`my-test-flow` を AM 10:00(JST) に実行するサンプルです。

```
deployments:
- name: my-test-flow
  tags: ["test"]
  description: "Test Flowです"
  schedule:
    cron: 0 10 * * *
    timezone: Asia/Tokyo
  flow_name: my-test-flow
  entrypoint: flows/sample.flow:overall_flow
  parameters:
    arg: "hello"
  work_pool:
    name: my-workpool
    job_variables: {}
```

### フロー実行のインフラストラクチャが分離している

フローは Prefect サーバ上で実行されません。
フローの実行方法の定義は、 `Work Pool` と呼び、例えば AWS の VPC や ECS Cluster の定義などを行います。

また、インフラストラクチャのプロビジョニングも行うことができ、 ECS Cluster や Task Definition などの作成も不要となります。
(実際の運用ではセキュリティ要件の問題で、別作成になると思いますが PoC や検証時はとても楽です)

実行環境は、AWS ECS / GCP CloudRun / Azure Container Instances / Docker / Kubenetes / Local Subprocess など、そして、 Prefect Managed が利用できます。

原則的には Worker と呼ばれる、スケジュールをポーリングしデプロイメントの実行をスケジュールするサービスの起動しておく必要があります。

ただし、Prefect Cloud では、Worker レスの Push Work Pool オプションがあり、その場合はサーバ側でスケジューリングされた後、実行環境のタスクが実行されます。

### 管理画面 がモダン

Dashboard では直近の実行結果の統計などが見られ、モダンなデザインとなっています（もちろんダークモード対応）。

![Prefect管理画面](/images/prefect-is-perfect/img1.png)

特筆すべきは、エラーとなった flow のログです。
Prefect Cloud では [Marvin](https://www.askmarvin.ai/) という AI により、実行ログから重要なエラーメッセージのみを選び表示してくれます。
