package com.gravity.textutils.analysis;

public class LegacyPhraseBoundary {
  private boolean canCross;
  private boolean canStart;
  private boolean canEnd;

  public LegacyPhraseBoundary() { }

  public boolean isCanCross() {
    return canCross;
  }

  public void setCanCross(boolean canCross) {
    this.canCross = canCross;
  }

  public boolean isCanStart() {
    return canStart;
  }

  public void setCanStart(boolean canStart) {
    this.canStart = canStart;
  }

  public boolean isCanEnd() {
    return canEnd;
  }

  public void setCanEnd(boolean canEnd) {
    this.canEnd = canEnd;
  }
}
