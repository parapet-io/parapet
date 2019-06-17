import React from "react";

import s from "./DistributedSystem.module.scss";

const DistributedSystem = () => {
  return (
    <div className={s.distributedWrapper}>
      <div className="mainContainer">
        <div className={s.distributedRow}>
          <div className={s.distributedLeft}>
            <h1>Build your distributed system</h1>
          </div>
          <div className={s.distributedRight}>
            <div className={s.distributedRightPart}>
              <div className={s.distributedRightItem}>
                <h3 className={s.distributedRightTitle}>Versatile</h3>
                <p className={s.distributedRightText}>
                  Parapet is a tool which provides all kinds of components that
                  you can use to develop a distributed systems of any complexity
                </p>
              </div>
              <div className={s.distributedRightItem}>
                <h3 className={s.distributedRightTitle}>
                  Advanced algorithmic tool box
                </h3>
                <p className={s.distributedRightText}>
                  Parapet isn't yet another back box framework it's a tool to
                  create distributed algorithms and ship them in composable
                  units called components
                </p>
              </div>
            </div>
            <div className={s.distributedRightPart}>
              <div className={s.distributedRightItem}>
                <h3 className={s.distributedRightTitle}>Friendly</h3>
                <p className={s.distributedRightText}>
                  Whether you a beginner or experienced distributed engineer you
                  will never find yourself limited in number of possibilities.
                  If you are a begginer and your goal is to build a distributed
                  system that satisfies your requirements you can use built-in
                  components or if you have a strong knowledge in distributed
                  computing you can start using low level components such as
                  Communication Links, Shared memory, Consistency models and
                  many other
                </p>
              </div>
              <div className={s.distributedRightItem}>
                <h3 className={s.distributedRightTitle}>Community Based</h3>
                <p className={s.distributedRightText}>
                  Implement your favorite distributed algorithms, pack them into
                  components and share with other engineers
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default DistributedSystem;
