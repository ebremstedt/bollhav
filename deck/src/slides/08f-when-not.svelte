<script>
  import Slide from '../lib/Slide.svelte'
</script>

<Slide kicker="Product market fit" title="Enough sales, why wouldn't I use it?" layout="wide">
  <div class="table">
    <table>
      <thead>
        <tr><th>When</th><th>Use instead</th></tr>
      </thead>
      <tbody>
        <tr><td>You use BigQuery, Snowflake, Databricks or Redshift</td><td>dbt, sqlmesh</td></tr>
        <tr><td>The team really does prefer SQL only*</td><td>dbt, sqlmesh</td></tr>
        <tr><td>You need events</td><td>Kafka, Debezium, Flink</td></tr>
        <tr><td>You want a traditional orchestrator</td><td>Bollhav models in combination with Airflow</td></tr>
        <tr><td>You don't want to write your connectors yourself</td><td>Fivetran, Airbyte, dlt</td></tr>
      </tbody>
    </table>
  </div>
  <p class="muted">
    The right fit: a team that values Python and has to manage many, many data products.
  </p>
  <p class="muted footnote">* <em>Bollhav supports SQL too, just not pure SQL models</em></p>
</Slide>

<style>
  .footnote {
    font-family: var(--font-mono);
    font-size: 0.85rem;
    letter-spacing: 0.02em;
  }
</style>
