﻿import stopIndexingCommand = require("commands/stopIndexingCommand");
import startIndexingCommand = require("commands/startIndexingCommand");
import getIndexingStatusCommand = require("commands/getIndexingStatusCommand");
import viewModelBase = require("viewmodels/viewModelBase");

class toggleIndexing extends viewModelBase {

  indexingStatus = ko.observable<string>("Started");
  modelPollingHandle: number;

  constructor() {
    super();

    this.modelPolling();
    this.modelPollingHandle = setInterval(() => this.modelPolling(), 5000);
  }

  disableIndexing() {
    new stopIndexingCommand(this.activeDatabase())
      .execute()
      .done(() => this.forceModelPolling());
  }

  enableIndexing() {
    new startIndexingCommand(this.activeDatabase())
      .execute()
      .done(() => this.forceModelPolling());
  }

  modelPolling() {
    new getIndexingStatusCommand(this.activeDatabase())
      .execute()
      .done(result=> this.indexingStatus(result.IndexingStatus));
  }

  forceModelPolling() {
    this.modelPolling();
  }

}

export = toggleIndexing;