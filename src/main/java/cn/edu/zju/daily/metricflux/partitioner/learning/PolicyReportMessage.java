package cn.edu.zju.daily.metricflux.partitioner.learning;

import java.io.Serializable;
import lombok.Data;

/** Message for the policy client to report the observation to the policy server. */
@Data
public class PolicyReportMessage implements Serializable {
    String episodeId;
    Object observation;
}
